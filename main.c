#define _GNU_SOURCE

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/wait.h>
#include <pthread.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

pthread_t *threads = NULL;
DIR *dir = NULL;

pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t thread_cond = PTHREAD_COND_INITIALIZER;
int active_threads = 0;
int running_backups = 0;
int max_backups = 0;

typedef struct {
    char *dir_path;
    char *job_file;
} job_t;

pthread_mutex_t job_mutex = PTHREAD_MUTEX_INITIALIZER;

void format_backup_path(char* dest_backup_path, char* file_path_no_ext, int backup_number) {
    memset(dest_backup_path, 0, MAX_PATH_SIZE);
    sprintf(dest_backup_path, "%s-%d.bck", file_path_no_ext, backup_number);
}

void* process_job_file(void* arg) {
    job_t *job = (job_t*)arg;

    char file_path[MAX_PATH_SIZE];
    char file_path_no_ext[MAX_PATH_SIZE];
    strcpy(file_path, job->dir_path);
    if (job->dir_path[strlen(job->dir_path) - 1] != '/') {
        strcat(file_path, "/");
    }
    strcat(file_path, job->job_file);

    // format output_path
    char output_path[MAX_PATH_SIZE];
    strcpy(output_path, file_path);
    char *dot_pos = strrchr(output_path, '.');
    if (dot_pos != NULL) {
        *dot_pos = '\0';
    }
    strcpy(file_path_no_ext, output_path);
    strcat(output_path, ".out");

    // open for reading input file
    int in_fd = open(file_path, O_RDONLY);
    if (in_fd == -1) {
        fprintf(stderr, "ERROR: Unable to open file '%s'\n", file_path);
        free(job);
        return NULL;
    }

    // output file
    int out_fd = open(output_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (out_fd == -1) {
        fprintf(stderr, "ERROR: Unable to open output file '%s' for writing\n", output_path);
        close(in_fd);
        free(job);
        return NULL;
    }

    int done = 0;
    int backup_count = 0;
    char backup_path[MAX_PATH_SIZE];

    while (!done) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        switch (get_next(in_fd)) {
            case CMD_WRITE:
                num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n");
                }
                break;

            case CMD_READ:
                num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_read(num_pairs, keys, out_fd)) {
                    fprintf(stderr, "Failed to read pair\n");
                }
                break;

            case CMD_DELETE:
                num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_delete(num_pairs, keys, out_fd)) {
                    fprintf(stderr, "Failed to delete pair\n");
                }
                break;

            case CMD_SHOW:
                kvs_show(out_fd);
                break;

            case CMD_WAIT:
                if (parse_wait(in_fd, &delay, NULL) == -1) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (delay > 0) {
                    const char *waiting = "Waiting...\n";
                    write(out_fd, waiting, strlen(waiting));
                    kvs_wait(delay);
                }
                break;

            case CMD_BACKUP:
                format_backup_path(backup_path, file_path_no_ext, backup_count+1);
                if (running_backups >= max_backups) {
                    wait(NULL);
                    running_backups -= 1;
                }

                if (kvs_backup(backup_path)) {
                    fprintf(stderr, "Failed to perform backup.\n");
                } else {
                    backup_count += 1;
                    running_backups += 1;
                }

                break;

            case CMD_INVALID:
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                break;

            case CMD_HELP:
                printf( 
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n"
                );
                break;

            case CMD_EMPTY:
                break;

            case EOC:
                close(in_fd);
                close(out_fd);
                done = 1;
                break;
        }
    }

    close(in_fd);
    close(out_fd);

    pthread_mutex_lock(&thread_mutex);
    active_threads--;
    pthread_cond_signal(&thread_cond);
    pthread_mutex_unlock(&thread_mutex);

    free(job->dir_path);
    free(job->job_file);
    free(job);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "USAGE: ./kvs <path_to_jobs> <max_backups> <max_threads>\n");
        return 1;
    }

    char *dir_path = argv[1];
    max_backups = atoi(argv[2]);
    int max_threads = atoi(argv[3]);

    if (access(dir_path, F_OK) == -1) {
        fprintf(stderr, "ERROR: file path '%s' does not exist\n", dir_path);
        return 1;
    }

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    dir = opendir(dir_path);
    if (!dir) {
        fprintf(stderr, "ERROR: Unable to open directory '%s'\n", dir_path);
        return 1;
    }

    threads = (pthread_t*)malloc((size_t)max_threads * sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "ERROR: Failed to allocate memory for threads\n");
        closedir(dir);
        return 1;
    }

    int thread_count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char *ext = strrchr(entry->d_name, '.');
        if (ext == NULL || strcmp(ext, ".job") != 0) {
            continue;
        }

        pthread_mutex_lock(&thread_mutex);
        while (active_threads >= max_threads) {
            // wait for the condition signal, meaning a thread has finished
            pthread_cond_wait(&thread_cond, &thread_mutex);

            for (int i = 0; i < thread_count; i++) {
                int result = pthread_tryjoin_np(threads[i], NULL);

                if (result == 0) { // successful join
                    for (int j = i; j < thread_count - 1; j++) {
                        threads[j] = threads[j + 1];
                    }
                    thread_count--;
                    i--;
                }
            }
        }
        pthread_mutex_unlock(&thread_mutex);

        job_t *job = malloc(sizeof(job_t));
        job->dir_path = strdup(dir_path);
        job->job_file = strdup(entry->d_name);

        printf("Processing job: %s\n", entry->d_name);

        pthread_create(&threads[thread_count], NULL, process_job_file, (void*)job);

        pthread_mutex_lock(&thread_mutex);
        active_threads++;
        pthread_mutex_unlock(&thread_mutex);

        thread_count++;
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);
    closedir(dir);
    kvs_terminate();
    return 0;
}
