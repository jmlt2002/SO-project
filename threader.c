//
// Created by dinis on 12/10/24.
//

#include "threader.h"
#include <malloc.h>
#include <string.h>
#define NEW(X) malloc(sizeof(X))
#define NEW_Z(N,X) calloc(N, sizeof(X))
/*
static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}
*/
void i_sleep() {
    struct timespec delay = (struct timespec){0, 1};
    nanosleep(&delay, NULL);
}

Job * job_create(void * (*func) (void*), void *args) {
    Job *job = NEW_Z(1, Job);
    job->func = func;
    job->args = args;
    pthread_mutex_init(&job->mtx, NULL);
    return job;
}

void job_delete(Job *job) {
    free(job);
}

void job_queue_item_delete(JobQueueItem * jqi) {
    free(jqi);
    // printf("DEBUG: jqi freed\n");
}

Job * job_queue_try_grab_job(JobQueue *jq) {
    Job * job = NULL;
    JobQueueItem *next_jqi;
    if (! pthread_mutex_trylock(& jq->mtx)) {
        if (! jq->tail) {
            pthread_mutex_unlock(&jq->mtx);
            return NULL;
        }

        next_jqi = jq->tail->next;
        job = jq->tail->job;
        job_queue_item_delete(jq->tail);
        jq->tail = next_jqi;

        if (! jq->tail) {
            jq->empty = 1;
        }
        pthread_mutex_unlock(&jq->mtx);
        // printf("DEBUG: job removed (Empty: %d)\n", jq->empty);
        return job;
    }
    return NULL;
}

void job_queue_init(JobQueue *jq) {
    pthread_mutex_init(&jq->mtx, NULL);
    jq->tail = NULL;
    jq->empty = 1;
}

int job_queue_count(JobQueue *jq) {
    int n = 0;
    JobQueueItem *jqi;
    for (jqi = jq->tail; jqi; jqi = jqi->next)
        n += 1;
    return n;
}

bool worker_grab_job(Worker* worker) {
    Job * job;

    if ( (! worker->thread_pool->jobs.empty) && (job = job_queue_try_grab_job(&worker->thread_pool->jobs)) ) {
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
            } else i_sleep(1);
        }
        cur_worker->current_job->func(cur_worker->current_job->args);
        job_delete(cur_worker->current_job);
        cur_worker->current_job = NULL;
    }

    return NULL;
}

void thread_pool_add_job(ThreadPool *tp, Job *job) {
    JobQueueItem * jqi = NEW(JobQueueItem);
    jqi->job = job;
    jqi->next = NULL;
    pthread_mutex_lock(&tp->jobs.mtx);

    if (!tp->jobs.empty) {
        tp->jobs.head->next = jqi;
        tp->jobs.head = jqi;
    }
    else {
        tp->jobs.tail = jqi;
        tp->jobs.head = jqi;
    }

    tp->jobs.empty = 0;
    pthread_mutex_unlock(&tp->jobs.mtx);
    // printf("DEBUG: job added (total: %d) \n", job_queue_count(&tp->jobs));
}

void thread_pool_init(ThreadPool *tp, int max_workers) {
    tp->max_workers = max_workers;
    tp->workers = NEW_Z((size_t) max_workers, Worker);  // alloc max_workers Worker(s), data set to [Z]ero
    /*tp->workers = malloc((size_t) (sizeof(Worker) * (size_t)max_workers));
    memset((void *) tp->workers, (int) 0, (unsigned long) (sizeof(Worker) * (size_t)max_workers));*/

    job_queue_init(&tp->jobs);

    for (int i=0;i<max_workers;i++) {
        tp->workers[i].thread_pool = tp;
        pthread_mutex_init(&tp->workers[i].mtx, NULL);
        pthread_create(&tp->workers[i].thread, NULL, worker_loop, (void *) &tp->workers[i]);
    }

}

void thread_pool_wait_all_done(ThreadPool *tp) {
    while (!tp->jobs.empty) i_sleep();
    for (int i=0; i<tp->max_workers; i++)
        while (tp->workers[i].current_job) i_sleep();
}

void thread_pool_kill(ThreadPool *tp) {
    int i;
    for (i=0; i<tp->max_workers; i++) tp->workers[i].kill = 1;
    for (i=0; i<tp->max_workers; i++) pthread_join(tp->workers[i].thread, NULL);
}

void thread_pool_destroy(ThreadPool *tp) {
    free(tp->workers);
}