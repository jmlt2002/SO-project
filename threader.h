//
// Created by dinis on 12/10/24.
//

#ifndef PROJ_1_THREADER_H
#define PROJ_1_THREADER_H
#include <pthread.h>

#define bool char

typedef struct job {
    bool done;
    void (*func)(void*);
    void *args;
    pthread_mutex_t mtx;
} Job;

typedef struct jobQueueItem {
    Job *job;
    struct jobQueueItem *prev;
} JobQueueItem;

typedef struct jobQueue {
    JobQueueItem *head;
    pthread_mutex_t mtx; // to wait for a new job
    bool empty;
} JobQueue;


typedef struct worker {
    bool kill;
    Job *current_job;
    struct threadPool *thread_pool;
    pthread_t thread;
    pthread_mutex_t mtx;
} Worker;

typedef struct threadPool {
    size_t max_workers;
    Worker *workers;
    JobQueue jobs;
} ThreadPool;


#endif //PROJ_1_THREADER_H
