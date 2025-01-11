#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"

#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>

const char *req_pipe_path_glob, *resp_pipe_path_glob, *notif_pipe_path_glob, *server_pipe_path_glob;
int server_pipe_glob, req_pipe_glob, resp_pipe_glob;

void cleanup() {
  printf("Cleaning up...\n");

  close(server_pipe_glob);
  close(req_pipe_glob);
  close(resp_pipe_glob);

  unlink(req_pipe_path_glob);
  unlink(resp_pipe_path_glob);
  unlink(notif_pipe_path_glob);
}

void add_to_message(char* message, const char* data, int start, int end) {
    for (int i = start; i < end; ++i, ++data) {
        message[i] = *data;
    }
}

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  
  if (mkfifo(resp_pipe_path, 0666) == -1 && errno != EEXIST) {
    fprintf(stderr, "Failed to create response pipe\n");
    return 1;
  }
  resp_pipe_path_glob = resp_pipe_path;

  if (mkfifo(notif_pipe_path, 0666) == -1 && errno != EEXIST) {
    fprintf(stderr, "Failed to create notification pipe\n");
    cleanup();
    return 1;
  }
  notif_pipe_path_glob = notif_pipe_path;

  if (mkfifo(req_pipe_path, 0666) == -1 && errno != EEXIST) {
    fprintf(stderr, "Failed to create request pipe\n");
    cleanup();
    return 1;
  }
  req_pipe_path_glob = req_pipe_path;

  int server_pipe = open(server_pipe_path, O_WRONLY);
  if (server_pipe == -1) {
    fprintf(stderr, "Failed to open register pipe\n");
    cleanup();
    return 1;
  }
  server_pipe_glob = server_pipe;

  // send register message to server pipe
  // (char) OP_CODE=1 | (char[40]) nome do pipe do cliente (para pedidos) | (char[40]) nome do pipe do cliente (para
  //      respostas) | (char[40]) nome do pipe do cliente (para notificações)
  char message[121];
  message[0] = OP_CODE_CONNECT;
  add_to_message(message, req_pipe_path, 1, 41);
  add_to_message(message, resp_pipe_path, 41, 81);
  add_to_message(message, notif_pipe_path, 81, 121);

  if (write(server_pipe, message, 121) == -1) {
    fprintf(stderr, "Failed to send register message\n");
    cleanup();
    return 1;
  }

  // open response pipe and wait for response
  // (char) OP_CODE=1 | (char) result
  int resp_pipe = open(resp_pipe_path, O_RDONLY);
  if (resp_pipe == -1) {
    fprintf(stderr, "Failed to open reponse pipe\n");
    cleanup();
    return 1;
  }
  resp_pipe_glob = resp_pipe;

  char response[2];
  if (read(resp_pipe, response, 2) == -1) {
    fprintf(stderr, "Failed to read response: connect\n");
    cleanup();
    return 1;
  }

  if (response[1] != SUCCESS) {
    fprintf(stdout, "Server returned 1 for operation: connect\n");
    cleanup();
    return 1;
  }

  printf("Server returned 0 for operation: connect\n");

  int req_pipe = open(req_pipe_path, O_WRONLY);
  if (req_pipe == -1) {
    fprintf(stderr, "Failed to open request pipe\n");
    cleanup();
    return 1;
  }
  req_pipe_glob = req_pipe;

  return 0;
}

int kvs_disconnect(void) {
  // OP_CODE_DISCONNECT = 2
  char message[1];
  message[0] = OP_CODE_DISCONNECT;
  if (write(req_pipe_glob, message, 1) == -1) {
    fprintf(stderr, "Failed to send disconnect message\n");
    cleanup();
    return 1;
  }

  char response[2];
  if (read(resp_pipe_glob, response, 2) == -1) {
    fprintf(stderr, "Failed to read response: disconnect\n");
    cleanup();
    return 1;
  } else if (response[1] != SUCCESS) {
    printf("response: %s\n", response);
    fprintf(stdout, "Server returned 1 for operation: disconnect\n");
    cleanup();
    return 1;
  }

  fprintf(stdout, "Server returned 0 for operation: disconnect\n");
  cleanup();
  return 0;
}

int kvs_subscribe(const char* key) {
  // OP_CODE_SUBSCRIBE = 3 | key[40] (padded with '\0')
  char message[41];
  message[0] = OP_CODE_SUBSCRIBE;
  add_to_message(message, key, 1, 41);
  if(write(req_pipe_glob, message, 41) == -1) {
    fprintf(stderr, "Failed to send subscribe message\n");
    cleanup();
    return 1;
  }

  char response[2];
  if (read(resp_pipe_glob, response, 2) == -1) {
    fprintf(stderr, "Failed to read response: subscribe\n");
    cleanup();
    return 1;
  } else if (response[1] != SUCCESS) {
    fprintf(stdout, "Server returned 1 for operation: subscribe\n");
    return 1;
  }

  fprintf(stdout, "Server returned 0 for operation: subscribe\n");
  return 0;
}

int kvs_unsubscribe(const char* key) {
  // OP_CODE_UNSUBSCRIBE = 4 | key[40] (padded with '\0')
  char message[41];
  message[0] = OP_CODE_UNSUBSCRIBE;
  add_to_message(message, key, 1, 41);
  if(write(req_pipe_glob, message, 41) == -1) {
    fprintf(stderr, "Failed to send unsubscribe message\n");
    cleanup();
    return 1;
  }

  char response[2];
  if (read(resp_pipe_glob, response, 2) == -1) {
      fprintf(stderr, "Failed to read response: unsubscribe\n");
      cleanup();
      return 1;
  } else if (response[0] != OP_CODE_UNSUBSCRIBE || response[1] != SUCCESS) {
      fprintf(stdout, "Server returned 1 for operation: unsubscribe\n");
      return 1;
  }

  fprintf(stdout, "Server returned 0 for operation: unsubscribe\n");
  return 0;
}
