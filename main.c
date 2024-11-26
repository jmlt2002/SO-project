#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

int main(int argc, char *argv[]) {

  if (argc != 3) {
      fprintf(stderr, "USAGE: ./kvs <path_to_job> <backup_number>\n");
      return 1;
  }

  char *file_path = argv[1];
  int max_backups = atoi(argv[2]);

  char *extension = strrchr(file_path, '.');
  if (strcmp(extension, ".job") != 0) {
      fprintf(stderr, "ERROR: file path must have a .job extension\n");
      return 1;
  }

  if (max_backups <= 0) {
      fprintf(stderr, "ERROR: maximum concurrent backups must be a positive number\n");
      return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  int in_fd = open(file_path, O_RDONLY);
  if (in_fd == -1) {
      fprintf(stderr, "ERROR: Unable to open file '%s'\n", file_path);
      return 1;
  }

  char *output_path = strdup(file_path);
  if (!output_path) {
      close(in_fd);
      return 1;
  }
  char *ext = strrchr(output_path, '.');
  strcpy(ext, ".out");

  int out_fd = open(output_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  if (out_fd == -1) {
      fprintf(stderr, "ERROR: Unable to open output file '%s' for writing\n", output_path);
      free(output_path);
      close(in_fd);
      return 1;
  }

  while (1) {
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

        if (kvs_backup()) {
          fprintf(stderr, "Failed to perform backup.\n");
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
            "  BACKUP\n" // Not implemented
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        kvs_terminate();
        close(in_fd);
        free(output_path);
        return 0;
    }
  }
}
