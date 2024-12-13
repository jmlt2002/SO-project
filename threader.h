//
// Created by dinis on 12/10/24.
//

#ifndef PROJ_1_THREADER_H
#define PROJ_1_THREADER_H
#include <pthread.h>

#define bool char

/**
 * Represents a job to be executed by a worker.
 */
typedef struct job {
    void * (*func) (void*);
    void *args;
} Job;

/**
 * Represents an item in the job queue.
 */
typedef struct jobQueueItem {
    Job *job;
    struct jobQueueItem *next;
} JobQueueItem;

/**
 * Represents a job queue.
 */
typedef struct jobQueue {
    JobQueueItem *tail;
    JobQueueItem *head;
    pthread_mutex_t mtx;
    bool empty;
} JobQueue;

/**
 * Represents a worker in the thread pool.
 */
typedef struct worker {
    bool kill;
    Job *current_job;
    struct threadPool *thread_pool;
    pthread_t thread;
    pthread_mutex_t mtx;
} Worker;

/**
 * Represents a thread pool.
 */
typedef struct threadPool {
    int max_workers;
    Worker *workers;
    JobQueue jobs;
} ThreadPool;

/**
 * Creates a new job.
 * @param func Pointer to the function the job will execute.
 * @param args Arguments for the job's function.
 * @return Pointer to the created Job.
 */
Job * job_create(void *(*func) (void*), void *args);

/**
 * Deletes a job and frees its resources.
 * @param job Pointer to the job to delete.
 */
void job_delete(Job *job);

/**
 * Counts the number of jobs in a job queue.
 * @param jq Pointer to the JobQueue.
 * @return Number of jobs in the queue.
 */
int job_queue_count(JobQueue *jq);

/**
 * Attempts to grab a job from the queue without blocking.
 * @param jq Pointer to the JobQueue.
 * @return Pointer to the grabbed Job, or NULL if the queue is empty or locked.
 */
Job * job_queue_try_grab_job(JobQueue *jq);

/**
 * Initializes the thread pool.
 * @param tp Pointer to the ThreadPool.
 * @param max_workers Maximum number of workers in the thread pool.
 */
void thread_pool_init(ThreadPool *tp, int max_workers);

/**
 * Adds a job to the thread pool's job queue.
 * @param tp Pointer to the ThreadPool.
 * @param job Pointer to the Job to add.
 */
void thread_pool_add_job(ThreadPool *tp, Job *job);

/**
 * Waits for all jobs in the thread pool to complete.
 * @param tp Pointer to the ThreadPool.
 */
void thread_pool_wait_all_done(ThreadPool *tp);

/**
 * Signals all workers in the thread pool to terminate and waits for them to finish.
 * @param tp Pointer to the ThreadPool.
 */
void thread_pool_kill(ThreadPool *tp);

/**
 * Destroys the thread pool and frees its resources.
 * @param tp Pointer to the ThreadPool.
 */
void thread_pool_destroy(ThreadPool *tp);

#endif //PROJ_1_THREADER_H
