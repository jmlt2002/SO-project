//
// Created by dinis on 12/10/24.
//

#ifndef PROJ_1_THREADER_H
#define PROJ_1_THREADER_H
#include <pthread.h>

#define bool char

typedef struct job {
    void * (*func) (void*);
    void *args;
    pthread_mutex_t mtx;
} Job;

typedef struct jobQueueItem {
    Job *job;
    struct jobQueueItem *next;
} JobQueueItem;

typedef struct jobQueue {
    JobQueueItem *tail;
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
    int max_workers;
    Worker *workers;
    JobQueue jobs;
} ThreadPool;

Job * job_create(void *func (void*), void *args);
void job_delete(Job *job);

int job_queue_count(JobQueue *jq);
Job * job_queue_try_grab_job(JobQueue *jq);
void thread_pool_init(ThreadPool *tp, int max_workers);
void thread_pool_add_job(ThreadPool *tp, Job *job);
void thread_pool_wait_all_done(ThreadPool *tp);
void thread_pool_kill(ThreadPool *tp);
void thread_pool_destroy(ThreadPool *tp);

#endif //PROJ_1_THREADER_H
