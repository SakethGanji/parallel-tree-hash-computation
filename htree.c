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
#include <assert.h>


uint32_t jenkins_one_at_a_time_hash(const uint8_t *, uint64_t);
double GetTime();
void Usage(char *); // Print out the usage of the program and exit.

// block size
#define BSIZE 4096

int main(int argc, char **argv) {
    int32_t fd;
    uint32_t nblocks;
    uint32_t mthreads;
    struct stat finfo;
    uint8_t *arr;

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

    arr = mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (arr == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    // calculate nblocks
    nblocks = fileSize / BSIZE;
    // calculate mthreads
    mthreads = atoi(argv[2]);

    printf(" no. of blocks = %u \n", nblocks);
    printf(" no. of threads = %u \n", mthreads);

    double start = GetTime();

    // calculate hash value of the input file


    double end = GetTime();
    //printf("hash value = %u \n", hash);
    printf("time taken = %f \n", (end - start));
    close(fd);
    return EXIT_SUCCESS;
}

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

double GetTime() {
    struct timeval t;
    int rc = gettimeofday(&t, NULL);
    assert(rc == 0);
    return (double)t.tv_sec + (double)t.tv_usec/1e6;
}

void Usage(char *s) {
    fprintf(stderr, "Usage: %s filename num_threads \n", s);
    exit(EXIT_FAILURE);
}
