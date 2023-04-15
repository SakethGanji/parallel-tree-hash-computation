#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>     // for EINTR
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <string.h>
#include <stdbool.h>

// struct for values passed to child threads
typedef struct blockInfo {
    uint8_t* mappedFile;
    int64_t mthreads;
    int64_t block;
    int64_t index;
} blockInfo;

uint32_t *initHashTree(uint8_t *mappedFile, int64_t mthreads, int64_t nblocks);
void createThreadSafe(pthread_t* threadPtr, void* (*funcPtr)(void*), void* argPtr, char* nodeName);
void joinThreadSafe(pthread_t thread, void** returnValue, char* nodeName);
void* computeHashTree(void* arg);
blockInfo initChildBlockInfo(blockInfo* parent, int64_t index);
uint32_t concatenateThenHash(uint32_t currentHash, const uint32_t* leftHash, const uint32_t* rightHash, bool rightNodeCanExist);
void* mallocSafe(size_t size, char* variableName);
uint32_t jenkins_one_at_a_time_hash(const uint8_t *, uint64_t);
double GetTime();
void Usage(char *);

#define BSIZE 4096 // block size

int main(int argc, char **argv) {
    int32_t fd;
    int nblocks;
    int mthreads;
    struct stat finfo;
    uint8_t *mappedFile;

    // input checking
    if (argc != 3)
        Usage(argv[0]);

    // open input file
    fd = open(argv[1], O_RDWR);
    if (fd == -1) {
        perror("open failed");
        exit(EXIT_FAILURE);
    }

    // use fstat to get file size
    if (fstat(fd, &finfo) == -1) {
        perror("fstat");
        exit(EXIT_FAILURE);
    }
    off_t fileSize = finfo.st_size;

    // map the file in address space
    mappedFile = mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mappedFile == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    // calculate nblocks
    nblocks = (int) (fileSize / BSIZE);

    // calculate mthreads and check for errors
    char *threadCountArg;
    mthreads = (int) strtol(argv[2], &threadCountArg, 10);
    // mthreads must be a positive integer
    if (*threadCountArg != '\0' || mthreads <= 0) {
        fprintf(stderr, "Error: Invalid number of threads. Must be positive integer.\n");
        exit(EXIT_FAILURE);
    }

    printf("num of threads = %u \n", mthreads);
    printf("blocks per thread = %u \n", (nblocks / mthreads));

    double start = GetTime();
    // create root thread for binary tree of threads
    uint32_t* hash = initHashTree(mappedFile, mthreads, nblocks);
    double end = GetTime();

    printf("hash value = %u \n", *hash);
    printf("time taken = %f \n", (end - start));

    // clean up
    close(fd);
    free(hash);
    munmap(mappedFile, fileSize);

    return EXIT_SUCCESS;
}

//initializes the binary tree of threads and managing the entire hashing process
uint32_t* initHashTree(uint8_t* mappedFile, int64_t mthreads, int64_t nblocks) {
    // initialize root node with starting values that will be passed on to the child threads
    blockInfo rootNode;
    rootNode.mappedFile = mappedFile;
    rootNode.mthreads = mthreads;
    rootNode.block = (nblocks / mthreads) * BSIZE;
    rootNode.index = 0;

    uint32_t *hash;
    pthread_t rootThread;
    // create and joins child threads, returns hash value of the file
    createThreadSafe(&rootThread, computeHashTree, &rootNode, "rootNode");
    joinThreadSafe(rootThread, (void **)&hash, "rootNode");

    return hash;
}

// computes hash values for assigned blocks and returns the hash values to their parent thread
void *computeHashTree(void* arg) {
    blockInfo *interiorNode = (blockInfo *) arg; // cast void* to blockInfo*
    // non-changing values
    int64_t mthreads = interiorNode->mthreads;
    int64_t block = interiorNode->block;
    uint8_t* mappedFile = interiorNode->mappedFile;

    // initialize left child node
    blockInfo leftNode = initChildBlockInfo(interiorNode, 2 * (interiorNode->index) + 1);
    bool leftNodeCanExist = leftNode.index < mthreads;
    // initialize right child node
    blockInfo rightNode = initChildBlockInfo(interiorNode, 2 * (interiorNode->index) + 2);
    bool rightNodeCanExist = rightNode.index < mthreads;

    // interior thread will compute the hash value of the assigned blocks
    uint32_t interiorHash = jenkins_one_at_a_time_hash(&mappedFile[(interiorNode->index) * block], block);
    uint32_t *concatenatedHash = (uint32_t *) mallocSafe(sizeof(uint32_t), "concatenatedHash");

    // if child node index is less than mthreads then create (only 0, 1 or 2) possible child threads
    // leaf thread computes the hash value of n/m consecutive blocks assigned
    if (!leftNodeCanExist && !rightNodeCanExist) { // base case (leaf node)
        *concatenatedHash = interiorHash;
        // return the hash value to its parent thread (through pthread_exit() call)
        pthread_exit(concatenatedHash);
    }
    // interior thread then computes hash value of the concatenated string
    // <computed hash value + hash value from left child + hash value from right child (optional)>
    if (leftNodeCanExist && rightNodeCanExist) { // creates two child threads
        pthread_t leftThread;
        pthread_t rightThread;
        void* leftHash;
        void* rightHash;
        createThreadSafe(&leftThread, computeHashTree, &leftNode, "leftNode");
        createThreadSafe(&rightThread, computeHashTree, &rightNode, "rightNode");
        joinThreadSafe(leftThread, &leftHash, "leftNode");
        joinThreadSafe(rightThread, &rightHash, "rightNode");
        *concatenatedHash = concatenateThenHash(interiorHash, leftHash, rightHash, true);
        free(leftHash);
        free(rightHash);

    }
    else if (leftNodeCanExist) { // if only one child possible then it can only be the left child
        pthread_t leftThread;
        void* leftHash;
        createThreadSafe(&leftThread, computeHashTree, &leftNode, "leftNode");
        joinThreadSafe(leftThread, &leftHash, "leftNode");
        *concatenatedHash = concatenateThenHash(interiorHash, leftHash, NULL, false);
        free(leftHash);
    }

    return concatenatedHash; // return hash value to parent thread
}

//initializes a new blockInfo struct with values from parent node and a specified index
blockInfo initChildBlockInfo(blockInfo* parentNode, int64_t index) {
    blockInfo node;
    node.mappedFile = parentNode->mappedFile;
    node.mthreads = parentNode->mthreads;
    node.block = parentNode->block;
    node.index = index;
    return node;
}

// wrapper for pthread_create that checks for errors and exits if unable to create thread
void createThreadSafe(pthread_t* threadPtr, void* (*funcPtr)(void*), void* argPtr, char* nodeName) {
    char *fullErrorMessage = (char *) mallocSafe(strlen("pthread_create ") + strlen(nodeName) + 1, "fullErrorMessage");
    sprintf(fullErrorMessage, "pthread_create %s", nodeName);
    // creates new thread with error checking with identifier nodeName for error message
    if (pthread_create(threadPtr, NULL, funcPtr, argPtr) != 0) {
        perror(fullErrorMessage);
        exit(EXIT_FAILURE);
    }
    free(fullErrorMessage);
}

// wrapper for pthread_join that checks for errors and exits if unable to join thread
void joinThreadSafe(pthread_t thread, void** returnValue, char* nodeName) {
    char *fullErrorMessage = (char *) mallocSafe(strlen("pthread_join ") + strlen(nodeName) + 1, "fullErrorMessage");
    sprintf(fullErrorMessage, "pthread_join %s", nodeName);
    // joins thread with error checking with identifier nodeName for error message
    if (pthread_join(thread, returnValue) != 0) {
        perror(fullErrorMessage);
        exit(EXIT_FAILURE);
    }
    free(fullErrorMessage);
}

// concatenates hash values of a parent node, left child node, and right child node(optional) and calculates the hash value
uint32_t concatenateThenHash(uint32_t currentHash, const uint32_t* leftHash, const uint32_t* rightHash, bool rightNodeCanExist) {
    uint64_t length;
    char* buffer;

    // use sprintf to convert in to string and concatenate hash values into a single string
    if (rightNodeCanExist == true) { //concatenate right hash value
        length = snprintf(NULL, 0, "%u%u%u", currentHash, *leftHash, *rightHash);
        buffer = (char *) mallocSafe(length + 1, "buffer");
        snprintf(buffer, length + 1, "%u%u%u", currentHash, *leftHash, *rightHash);
    }
    else { // don't concatenate right hash value
        length = snprintf(NULL, 0, "%u%u", currentHash, *leftHash);
        buffer = (char *) mallocSafe(length + 1, "buffer");
        snprintf(buffer, length + 1, "%u%u", currentHash, *leftHash);
    }
    uint32_t concatenatedHash = jenkins_one_at_a_time_hash((uint8_t *) buffer, length);
    free(buffer);

    return concatenatedHash;
}

// wrapper for malloc that checks for errors and exits if malloc fails
void* mallocSafe(size_t size, char* variableName) {
    void* ptr = malloc(size);
    if (ptr == NULL) {
        printf("Error: failed to allocate memory for %s\n", variableName);
        exit(EXIT_FAILURE);
    }
    return ptr;
}

// implements the Jenkins one-at-a-time hash algorithm on an array
uint32_t jenkins_one_at_a_time_hash(const uint8_t *key, uint64_t length) {
    uint64_t i = 0;
    uint32_t hash = 0;

    while (i != length) {
        hash += key[i++];
        hash += hash << 10;
        hash ^= hash >> 6;
    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}

// returns the current time in seconds
double GetTime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (double) t.tv_sec + (double) t.tv_usec / 1e6;
}

// called when the program is executed with an incorrect number of arguments
void Usage(char *s) {
    fprintf(stderr, "Usage: %s filename num_threads \n", s);
    exit(EXIT_FAILURE);
}
