#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

typedef struct {
    char* notif_pipe_path;
    pthread_mutex_t* lock;
    int* is_running;
} notif_thread_data_t;

void* notification_handler(void* arg) {
    notif_thread_data_t* data = (notif_thread_data_t*)arg;
    char buffer[MAX_STRING_SIZE];
    char key[MAX_STRING_SIZE];
    int is_value = 0;

    int notif_pipe = open(data->notif_pipe_path, O_RDONLY);
    if (notif_pipe == -1) {
        perror("Failed to open notification pipe");
        return NULL;
    }

    while (1) {
        pthread_mutex_lock(data->lock);
        if (!(*data->is_running)) {
            pthread_mutex_unlock(data->lock);
            break;
        }
        pthread_mutex_unlock(data->lock);
        
        ssize_t bytes_read = read(notif_pipe, buffer, MAX_STRING_SIZE);
        if (bytes_read > 0 && is_value) {
            printf("(%s,%s)\n", key, buffer);
            is_value = 0;
        } else if (bytes_read > 0 && !is_value) {
            strncpy(key, buffer, MAX_STRING_SIZE);
            key[MAX_STRING_SIZE] = '\0';
            is_value = 1;
        } else if (bytes_read == 0) {
            // Pipe closed
            break;
        } else {
            perror("Error reading from notification pipe");
        }
    }

    close(notif_pipe);
    return NULL;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[40] = "/tmp/req";
  char resp_pipe_path[40] = "/tmp/resp";
  char notif_pipe_path[40] = "/tmp/notif";
  char register_pipe_path[40];
  strncpy(register_pipe_path, argv[2], sizeof(register_pipe_path) - 1);
  register_pipe_path[sizeof(register_pipe_path) - 1] = '\0';

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // pad pipe paths with '\0'
  for (size_t i = strlen(req_pipe_path); i < 40; ++i) {
    req_pipe_path[i] = '\0';
  }

  for (size_t i = strlen(resp_pipe_path); i < 40; ++i) {
    resp_pipe_path[i] = '\0';
  }

  for (size_t i = strlen(notif_pipe_path); i < 40; ++i) {
    notif_pipe_path[i] = '\0';
  }
  
  int ret = kvs_connect(req_pipe_path, resp_pipe_path, register_pipe_path, notif_pipe_path);
  if (ret != 0) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }

  pthread_mutex_t lock;
  pthread_mutex_init(&lock, NULL);
  int is_running = 1;
  notif_thread_data_t data = {notif_pipe_path, &lock, &is_running};
  pthread_t notif_thread;
  pthread_create(&notif_thread, NULL, notification_handler, &data);

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        kvs_disconnect();

        pthread_mutex_lock(&lock);
        is_running = 0;
        pthread_mutex_unlock(&lock);
        pthread_join(notif_thread, NULL);
        printf("Disconnected from server\n");
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        kvs_subscribe(keys[0]);
        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        kvs_unsubscribe(keys[0]);
        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
}
