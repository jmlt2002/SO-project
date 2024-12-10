//
// Created by dinis on 12/10/24.
//

#include "threader.h"
#include <malloc.h>

#define NEW(X) malloc(sizeof(X))
#define NEW_Z(N,X) calloc(N, sizeof(X))

static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void ms_sleep(unsigned int delay_ms) {
    struct timespec delay = delay_to_timespec(delay_ms);
    nanosleep(&delay, NULL);
}


void job_delete(Job *job) {
    free(job);
}

void job_queue_item_delete(JobQueueItem * jqi) {
    job_delete(jqi->job);
    free(jqi);
}

Job * job_queue_try_grab_job(JobQueue *jq) {
    Job * job = NULL;
    JobQueueItem *prev_jqi;
    if (! pthread_mutex_trylock(& jq->mtx)) {
        if (! jq->head) {
            pthread_mutex_unlock(&jq->mtx);
            return NULL;
        }
        job = jq->head->prev->job;
        prev_jqi = jq->head->prev;
        job_queue_item_delete(jq->head);
        jq->head = prev_jqi;
        if (! jq->head) jq->empty = 1;
        pthread_mutex_unlock(&jq->mtx);
        return job;
    }
    return NULL;
}

void job_queue_init(JobQueue *jq) {
    pthread_mutex_init(&jq->mtx, NULL);
    jq->head = NULL;
    jq->empty = 1;
}

bool worker_grab_job(Worker* worker) {
    Job * job;
    if ( (! worker->thread_pool->jobs.empty) && (job = job_queue_try_grab_job(&worker->thread_pool->jobs)) ) {
        job_delete(worker->current_job);
        worker->current_job = job;
        return 1;
    }
    return 0;
}

void *worker_loop(void *arg) {
    Worker *cur_worker = (Worker *) arg;

    while (! cur_worker->kill) {
        while (! worker_grab_job(cur_worker) ) {
            if (cur_worker->kill) {
                return NULL;
            } else ms_sleep(1);
        }
        cur_worker->current_job->func(cur_worker->current_job->args);
    }

    return NULL;
}

void thread_pool_add_job(ThreadPool *tp, Job *job) {
    JobQueueItem * jqi = NEW(JobQueueItem);
    jqi->job = job;
    pthread_mutex_lock(&tp->jobs.mtx);
    jqi->prev = tp->jobs.head;
    tp->jobs.head = jqi;
    tp->jobs.empty = 0;
    pthread_mutex_unlock(&tp->jobs.mtx);
}

void thread_pool_init(ThreadPool *tp, int max_workers) {
    tp->max_workers = max_workers;
    tp->workers = NEW_Z(max_workers, Worker);  // alloc max_workers Worker(s), data set to [Z]ero
    tp = NEW(ThreadPool);
    job_queue_init(&tp->jobs);

    for (int i=0;i<max_workers;i++) {
        tp->workers[i].thread_pool = tp;
        pthread_mutex_init(&tp->workers[i].mtx, NULL);
        pthread_create(&tp->workers[i].thread, NULL, worker_loop, (void *) &tp->workers[i]);
    }

}

void thread_pool_kill(ThreadPool *tp) {
    int i;
    for (i=0; i<tp->max_workers; i++) tp->workers[i].kill = 1;
    for (i=0; i<tp->max_workers; i++) pthread_join(tp->workers[i].thread, NULL);
}

void thread_pool_destroy(ThreadPool *tp) {
    free(tp->workers);
}