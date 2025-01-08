#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "subscriptions.h"
#include "pc_buffer.h"
#include "src/common/protocol.h"
#include "src/common/constants.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct {
  Buffer buffer;
  sem_t semaphore;
  pthread_mutex_t mutex;
  int active_sessions;
} PCBuffer;

pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
int active_threads = 0;        // Number of active threads
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;
// TODO: add mutexes to active_threads and active_sessions
PCBuffer pc_buffer;

BufferData process_register_message(const char *message_buffer) {
  BufferData output = {NULL, NULL, NULL};

  strncpy(output.request_pipe, message_buffer + 1, MAX_PIPE_PATH_LENGTH);
  output.request_pipe[MAX_PIPE_PATH_LENGTH] = '\0';

  strncpy(output.response_pipe, message_buffer + 1 + MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);
  output.response_pipe[MAX_PIPE_PATH_LENGTH] = '\0';

  strncpy(output.notification_pipe, message_buffer + 1 + 2 * MAX_PIPE_PATH_LENGTH, MAX_PIPE_PATH_LENGTH);
  output.notification_pipe[MAX_PIPE_PATH_LENGTH] = '\0';

  return output;
}

int notify(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], int deleted) {
  char* key = NULL;
  char* value = NULL;
  
  for (size_t i = 0; i < num_pairs; i++) {
    key = keys[i];
    if(values != NULL)
      value = values[i];

    char key_buffer[41];
    strncpy(key_buffer, key, strlen(key));
    for (size_t j = strlen(key); j < 41; j++) {
      key_buffer[j] = '\0';
    }

    char value_buffer[41];
    if (values != NULL){
      strncpy(value_buffer, value, strlen(value));
      for (size_t k = strlen(value); k < 41; k++) {
        value_buffer[k] = '\0';
      }
    }

    InnerNode* current = findKey(key);
    while (current != NULL) {
      int pipe = open(current->notification_pipe, O_WRONLY);
      if (pipe == -1) {
        fprintf(stderr, "Failed to open pipe %s\n", current->notification_pipe);
        return -1;
      }

      write(pipe, key_buffer, 41); 
      if(deleted) {
        // DELETED padded with '\0' until 41 total characters
        char deleted_message[41] = {0};
        snprintf(deleted_message, sizeof(deleted_message), "DELETED");
        write(pipe, deleted_message, 41);
      } else {
        write(pipe, value_buffer, 41);
      }

      close(pipe);
      current = current->next;
    }
  }
  return 0;
}

static void *manage_subscriptions() {
  int current_subscriptions = 0;
  int response_pipe, request_pipe;

  while (1) {
    sem_wait(&pc_buffer.semaphore);

    pthread_mutex_lock(&pc_buffer.mutex);
    if(pc_buffer.active_sessions >= MAX_SESSION_COUNT) {
      pthread_mutex_unlock(&pc_buffer.mutex);
      continue;
    } else {
      pc_buffer.active_sessions++;
    }

    BufferData args = removeFromBuffer(&pc_buffer.buffer);
    if (args.request_pipe == NULL) {
      pc_buffer.active_sessions--;
      pthread_mutex_unlock(&pc_buffer.mutex);
      continue;
    }
    pthread_mutex_unlock(&pc_buffer.mutex);

    response_pipe = open(args.response_pipe, O_WRONLY);
    if (response_pipe == -1) {
      fprintf(stderr, "Failed to open response pipe %s\n", args.response_pipe);
      pthread_exit(NULL);
    }
    write_str(response_pipe, "10");

    request_pipe = open(args.request_pipe, O_RDONLY);
    if (request_pipe == -1) {
      fprintf(stderr, "Failed to open request pipe %s\n", args.request_pipe);
      close(response_pipe);
      pthread_exit(NULL);
    }

    char buffer[41];
    ssize_t bytes_read;
    while (1) {
      bytes_read = read(request_pipe, buffer, 41);
      if (bytes_read == -1) {
        fprintf(stderr, "Failed to read from pipe\n");
        break;
      }

      if (bytes_read < 41 && bytes_read > 0) {
        if(buffer[0] == OP_CODE_DISCONNECT) {
          // disconnect
          break;
        }
      } else if (bytes_read == 41) {
        if(buffer[0] == OP_CODE_SUBSCRIBE) {
          // subscribe
          char key[40];
          strncpy(key, &buffer[1], 40);
          key[39] = '\0';

          // response
          if(current_subscriptions >= MAX_NUMBER_SUB || !kvs_find_key(key)) {
            char message[2];
            message[0] = OP_CODE_SUBSCRIBE;
            message[1] = FAILURE;
            write(response_pipe, message, 2);
          } else {
            char message[2];
            message[0] = OP_CODE_SUBSCRIBE;
            message[1] = SUCCESS;
            write(response_pipe, message, 2);
            addToList(key, args.notification_pipe);
            current_subscriptions++;
          }
        } else if(buffer[0] == OP_CODE_UNSUBSCRIBE) {
          // unsubscribe
          char key[40];
          strncpy(key, &buffer[1], 40);
          key[39] = '\0';
          removeFromList(key, args.notification_pipe);

          // response
          char message[2];
          message[0] = OP_CODE_UNSUBSCRIBE;
          message[1] = SUCCESS;
          write(response_pipe, message, 2);
          current_subscriptions--;
        }
      }
    }

    // TODO: check disconnect logic
    write_str(response_pipe, "20");
    close(request_pipe);
    close(response_pipe);
    pthread_mutex_lock(&pc_buffer.mutex);
    pc_buffer.active_sessions--;
    pthread_mutex_unlock(&pc_buffer.mutex);
  }

  write_str(response_pipe, "20");
  close(request_pipe);
  close(response_pipe);
  pthread_exit(NULL);
}

static void dispatch_session_threads() {
  // TODO: check if detach logic is correct

  for (size_t i = 0; i < MAX_SESSION_COUNT; i++) {
    pthread_t thread;
    if (pthread_create(&thread, NULL, manage_subscriptions, NULL) != 0) {
      fprintf(stderr, "Failed to create session thread\n");
      break;
    }
    pthread_detach(thread);
  }
}

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        } else if (notify(num_pairs, keys, values, 0)) {
          write_str(STDERR_FILENO, "Failed to notify write\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        } else if (notify(num_pairs, keys, NULL, 1)) {
          write_str(STDERR_FILENO, "Failed to notify delete\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  active_threads--;
  pthread_exit(NULL);
}

static void dispatch_job_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    } else {
      active_threads++;
    }
  }

  // FIXME: main function will not progress after dispatch_threads because of this join loop
  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int main(int argc, char** argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
		write_str(STDERR_FILENO, " <register_pipe_path> \n");
    return 1;
  }

  jobs_directory = argv[1];

  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

	if (max_backups <= 0) {
		write_str(STDERR_FILENO, "Invalid number of backups\n");
		return 0;
	}

	if (max_threads <= 0) {
		write_str(STDERR_FILENO, "Invalid number of threads\n");
		return 0;
	}

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  // create and open the register pipe
  char register_pipe_path[40];
  strncpy(register_pipe_path, argv[4], sizeof(register_pipe_path) - 1);
  register_pipe_path[sizeof(register_pipe_path) - 1] = '\0';
  if(mkfifo(register_pipe_path, 0666) == -1 && errno != EEXIST) {
    fprintf(stderr, "Failed to create register pipe\n");
    return 1;
  }

  int register_pipe = open(register_pipe_path, O_RDONLY);
  if (register_pipe == -1) {
    fprintf(stderr, "Failed to open register pipe\n");
    return 1;
  }

  initBuffer(&pc_buffer.buffer);
  sem_init(&pc_buffer.semaphore, 0, 1);
  pthread_mutex_init(&pc_buffer.mutex, NULL);
  pc_buffer.active_sessions = 0;

  dispatch_session_threads();
  dispatch_job_threads(dir);

  while(1) {
    int bytes_read;
    char register_message[121];
    BufferData processed_registry;
    while ((bytes_read = (int)read(register_pipe, register_message, 121)) > 0) {
      if (bytes_read == 121 && register_message[0] == OP_CODE_CONNECT) {
        processed_registry = process_register_message(register_message);
      } else {
        fprintf(stderr, "Invalid message received\n");
        continue;
      }

      insertInBuffer(&pc_buffer.buffer, processed_registry);
      sem_post(&pc_buffer.semaphore);
    }

    if (active_threads == 0) {
      break;
    }
  }
  
  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  sem_destroy(&pc_buffer.semaphore); // Destroy the semaphore
  pthread_mutex_destroy(&pc_buffer.mutex);
  kvs_terminate();

  return 0;
}
