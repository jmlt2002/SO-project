#ifndef KVS_CONSTANTS_H
#define KVS_CONSTANTS_H

#define MAX_WRITE_SIZE 256
#define MAX_STRING_SIZE 40
#define MAX_JOB_FILE_NAME_SIZE 256
#define MAX_PATH_SIZE 4096

#include <pthread.h>
#include <dirent.h>

typedef struct {
    pthread_t thread;
    int is_finished;
} Thread;

extern Thread *threads;
extern DIR *dir;

#endif