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
    void * (*func) (void*); ///< Function pointer to the job's task.
    void *args; ///< Arguments for the job's task.
    pthread_mutex_t mtx; ///< Mutex for job synchronization.
} Job;

/**
 * Represents an item in the job queue.
 */
typedef struct jobQueueItem {
    Job *job; ///< Pointer to the job.
    struct jobQueueItem *next; ///< Pointer to the next item in the queue.
} JobQueueItem;

/**
 * Represents a job queue.
 */
typedef struct jobQueue {
    JobQueueItem *tail; ///< Pointer to the tail of the queue.
    JobQueueItem *head; ///< Pointer to the head of the queue.
    pthread_mutex_t mtx; ///< Mutex for queue synchronization.
    bool empty; ///< Boolean indicating whether the queue is empty.
} JobQueue;

/**
 * Represents a worker in the thread pool.
 */
typedef struct worker {
    bool kill; ///< Flag to indicate if the worker should terminate.
    Job *current_job; ///< The current job being executed by the worker.
    struct threadPool *thread_pool; ///< Pointer to the thread pool the worker belongs to.
    pthread_t thread; ///< The thread managed by this worker.
    pthread_mutex_t mtx; ///< Mutex for worker synchronization.
} Worker;

/**
 * Represents a thread pool.
 */
typedef struct threadPool {
    int max_workers; ///< Maximum number of workers in the pool.
    Worker *workers; ///< Array of workers in the pool.
    JobQueue jobs; ///< Job queue for the pool.
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
