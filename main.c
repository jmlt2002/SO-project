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
#include <semaphore.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

sem_t semaphore;
int running_backups = 0;

void format_backup_path(char* dest_backup_path, char* file_path_no_ext, int backup_number) {
    memset(dest_backup_path, 0, MAX_PATH_SIZE);
    sprintf(dest_backup_path, "%s-%d.bck", file_path_no_ext, backup_number);
}

// thread function to process each job file
void* process_job_file(void* arg) {
    char** args = (char**)arg;
    char* dir_path = args[0];
    char* job_file = args[1];
    int max_backups = atoi(args[2]);

    printf("Thread started\n");

    char file_path[MAX_PATH_SIZE];
    char file_path_no_ext[MAX_PATH_SIZE];
    strcpy(file_path, dir_path);
    if (dir_path[strlen(dir_path) - 1] != '/') {
        strcat(file_path, "/");
    }
    strcat(file_path, job_file);

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
        return NULL;
    }

    // output file
    int out_fd = open(output_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (out_fd == -1) {
        fprintf(stderr, "ERROR: Unable to open output file '%s' for writing\n", output_path);
        close(in_fd);
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

    free(dir_path);
    free(job_file);
    free(args[2]);
    free(args);

    sem_post(&semaphore);

    printf("Thread finished\n");

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "USAGE: ./kvs <path_to_jobs> <backup_number> <max_threads>\n");
        return 1;
    }

    char *dir_path = argv[1];
    int max_backups = atoi(argv[2]);
    int max_threads = atoi(argv[3]);

    if (access(dir_path, F_OK) == -1) {
        fprintf(stderr, "ERROR: file path '%s' does not exist\n", dir_path);
        return 1;
    }

    if (max_backups <= 0) {
        fprintf(stderr, "ERROR: maximum concurrent backups must be a positive number\n");
        return 1;
    }

    if (sem_init(&semaphore, 0, (unsigned int)max_threads) != 0) {
        fprintf(stderr, "ERROR: Failed to initialize semaphore\n");
        return 1;
    }

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    DIR *dir = opendir(dir_path);
    if (!dir) {
        fprintf(stderr, "ERROR: Unable to open directory '%s'\n", dir_path);
        sem_destroy(&semaphore);
        return 1;
    }

    struct dirent *entry;
    pthread_t *threads = (pthread_t*)malloc((size_t)max_threads * sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "ERROR: Failed to allocate memory for threads\n");
        sem_destroy(&semaphore);
        closedir(dir);
        return 1;
    }

    int thread_count = 0;

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char *ext = strrchr(entry->d_name, '.');
        if (ext == NULL || strcmp(ext, ".job") != 0) {
            continue;
        }

        sem_wait(&semaphore);

        // allocate memory for the thread arguments
        char **args = malloc(3 * sizeof(char *));
        if (args == NULL) {
            fprintf(stderr, "ERROR: Failed to allocate memory for thread arguments\n");
            sem_post(&semaphore);
            continue;
        }

        args[0] = strdup(dir_path);
        args[1] = strdup(entry->d_name);
        args[2] = strdup(argv[2]);

        if (!args[0] || !args[1] || !args[2]) {
            fprintf(stderr, "ERROR: Failed to duplicate string for thread arguments\n");
            free(args[0]);
            free(args[1]);
            free(args[2]);
            free(args);
            sem_post(&semaphore);
            continue;
        }

        printf("Processing job: %s\n", entry->d_name);

        pthread_create(&threads[thread_count], NULL, process_job_file, (void*)args);
        thread_count++;
        int finished = 0;
        while(1) {
            for (int i = 0; i < thread_count; i++) {
                int result = pthread_tryjoin_np(threads[i], NULL);
                if (result == 0) {
                    for (int j = i; j < thread_count - 1; j++) {
                        threads[j] = threads[j + 1];
                    }
                    thread_count--;
                    i--;
                    finished = 1;
                    break;
                }
            }
            // if no thread was joined, wait 1sec before trying again
            if (thread_count == max_threads) {
                sleep(1);
            }

            if (finished) {
                finished = 0;
                break;
            }
        }
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);

    sem_destroy(&semaphore);
    closedir(dir);
    kvs_terminate();
    return 0;
}
