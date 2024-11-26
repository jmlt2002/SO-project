#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

int main(int argc, char *argv[]) {

  if (argc != 3) {
    fprintf(stderr, "USAGE: ./kvs <path_to_jobs> <backup_number>\n");
    return 1;
  }

  char *dir_path = argv[1];
  int max_backups = atoi(argv[2]);

  if (access(dir_path, F_OK) == -1) {
    fprintf(stderr, "ERROR: file path '%s' does not exist\n", dir_path);
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

  DIR *dir = opendir(dir_path);
  if (!dir) {
    fprintf(stderr, "ERROR: Unable to open directory '%s'\n", dir_path);
    kvs_terminate();
    return 1;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    char *ext = strrchr(entry->d_name, '.');
    if (ext == NULL || strcmp(ext, ".job") != 0) {
      continue;
    }

    // format input file_path
    char file_path[MAX_PATH_SIZE];
    strcpy(file_path, dir_path);
    if (dir_path[strlen(dir_path) - 1] != '/') {
      strcat(file_path, "/");
    }
    strcat(file_path, entry->d_name);

    // format output_path
    char output_path[MAX_PATH_SIZE];
    strcpy(output_path, file_path);
    char *dot_pos = strrchr(output_path, '.');
    if (dot_pos != NULL) {
      *dot_pos = '\0';
    }
    strcat(output_path, ".out");

    // open for reading input file
    int in_fd = open(file_path, O_RDONLY);
    if (in_fd == -1) {
      fprintf(stderr, "ERROR: Unable to open file '%s'\n", file_path);
      continue;
    }

    // output file
    int out_fd = open(output_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (out_fd == -1) {
      fprintf(stderr, "ERROR: Unable to open output file '%s' for writing\n", output_path);
      close(in_fd);
      continue;
    }

    int done = 0;
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
  }
  kvs_terminate();
  closedir(dir);
  return 0;
}
