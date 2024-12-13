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
#include "threader.h"

#define NEW(X) malloc(sizeof(X))
int MAX_THREADS;
#define SINGLE_THREADED (MAX_THREADS == 1)
static int MAX_CONCURRENT_BACKUPS;
static int RUNNING_BACKUPS = 0;
static pthread_mutex_t MTX_RUNNING_BACKUPS = PTHREAD_MUTEX_INITIALIZER;

void format_backup_path(char *dest_backup_path, char *file_path_no_ext, int backup_number) {
    memset(dest_backup_path, 0, MAX_PATH_SIZE);

    sprintf(dest_backup_path, "%s-%d.bck", file_path_no_ext, backup_number);
}

void *process_file(int in_fd, int out_fd, char *file_path_no_ext) {
    int backup_count = 0;
    char backup_path[MAX_PATH_SIZE];
    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;



        switch (get_next(in_fd)) {
            case CMD_WRITE:
            printf("[%lu] write\n", pthread_self());
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
            printf ("[%lu] read\n", pthread_self());
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
            printf("[%lu] delete\n", pthread_self());
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
            printf("[%lu] show\n", pthread_self());
                kvs_show(out_fd, 0);

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
            printf("[%lu] backup\n", pthread_self());
                format_backup_path(backup_path, file_path_no_ext, backup_count + 1);
                pthread_mutex_lock(&MTX_RUNNING_BACKUPS);
                if (RUNNING_BACKUPS >= MAX_CONCURRENT_BACKUPS) {

                    wait(NULL);
                    RUNNING_BACKUPS -= 1;
                    pthread_mutex_unlock(&MTX_RUNNING_BACKUPS);
                } else
                    pthread_mutex_unlock(&MTX_RUNNING_BACKUPS);

                if (kvs_backup(backup_path)) {
                    fprintf(stderr, "Failed to perform backup.\n");
                } else {
                    backup_count += 1;  // doesn't need mutex, local to the cur thread.
                    pthread_mutex_lock(&MTX_RUNNING_BACKUPS);
                    RUNNING_BACKUPS += 1;
                    pthread_mutex_unlock(&MTX_RUNNING_BACKUPS);
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
                return NULL;
        }

    }
}

int execute_job_file(char *d_name, char *dir_path) {
    // format input file_path
    char file_path[MAX_PATH_SIZE];
    char file_path_no_ext[MAX_PATH_SIZE];
    strcpy(file_path, dir_path);
    if (dir_path[strlen(dir_path) - 1] != '/') {
        strcat(file_path, "/");
    }
    strcat(file_path, d_name);

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
        return -1;
    }

    // output file
    int out_fd = open(output_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (out_fd == -1) {
        fprintf(stderr, "ERROR: Unable to open output file '%s' for writing\n", output_path);
        close(in_fd);
        return -1;
    }

    process_file(in_fd, out_fd, file_path_no_ext);

    close(in_fd);
    close(out_fd);
    return 0;
}

struct twExecuteJobFileArgs {
    char *d_name;
    char *dir_path;
};

void *thread_wrapper_execute_job_file(void *arg) {
    struct twExecuteJobFileArgs *exec_job_args = (struct twExecuteJobFileArgs *) arg;
    execute_job_file(exec_job_args->d_name, exec_job_args->dir_path);
    free(exec_job_args->d_name);
    free(exec_job_args);
    return NULL;
}

int main(int argc, char *argv[]) {
    ThreadPool thread_pool;
    char *dir_path;
    DIR *dir;
    struct dirent *entry;
    struct twExecuteJobFileArgs *exec_job_args;
    Job *job;

    if (argc != 4) {
        fprintf(stderr, "USAGE: ./kvs <path_to_jobs> <backup_number> <max_threads>\n");
        return 1;
    }

    dir_path = argv[1];
    MAX_CONCURRENT_BACKUPS = atoi(argv[2]);
    MAX_THREADS = atoi(argv[3]);

    if (!SINGLE_THREADED) thread_pool_init(&thread_pool, MAX_THREADS-1);

    if (access(dir_path, F_OK) == -1) {
        fprintf(stderr, "ERROR: file path '%s' does not exist\n", dir_path);
        return 1;
    }

    if (MAX_CONCURRENT_BACKUPS <= 0) {
        fprintf(stderr, "ERROR: maximum concurrent backups must be a positive number\n");
        return 1;
    }

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    dir = opendir(dir_path);
    if (!dir) {
        fprintf(stderr, "ERROR: Unable to open directory '%s'\n", dir_path);
        kvs_terminate();
        return 1;
    }


    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char *ext = strrchr(entry->d_name, '.');
        if (ext == NULL || strcmp(ext, ".job") != 0) {
            continue;
        }

        if (SINGLE_THREADED) {
            execute_job_file(entry->d_name, dir_path);
        } else {
            exec_job_args = NEW(struct twExecuteJobFileArgs);
            exec_job_args->d_name = strdup(entry->d_name);
            exec_job_args->dir_path = dir_path;
            job = job_create(thread_wrapper_execute_job_file, (void *) exec_job_args);
            thread_pool_add_job(&thread_pool, job);
        }

    }
    closedir(dir);

    // wait for child processes to finish
    while (wait(NULL) > 0);


    // TODO: make the main thread also work

    if (!SINGLE_THREADED) {
        while (!thread_pool.jobs.empty) {
            Job * j = job_queue_try_grab_job(&thread_pool.jobs);
            if (j) {
                j->func(j->args);
                job_delete(j);
            }
        }
        thread_pool_wait_all_done(&thread_pool);
        thread_pool_kill(&thread_pool);
        thread_pool_destroy(&thread_pool);
    }

    kvs_terminate();


    return 0;
}
