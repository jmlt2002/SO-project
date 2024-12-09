#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/wait.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

void format_backup_path(char* dest_backup_path, char* file_path_no_ext, int backup_number) {
    memset(dest_backup_path, 0, MAX_PATH_SIZE);

    sprintf(dest_backup_path, "%s-%d.bck", file_path_no_ext, backup_number);
}

int main(int argc, char *argv[]) {

  if (argc != 3) {
    fprintf(stderr, "USAGE: ./kvs <path_to_jobs> <backup_number>\n");
    return 1;
  }

  char *dir_path = argv[1];
  int max_backups = atoi(argv[2]);
  int running_backups = 0;
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
    char file_path_no_ext[MAX_PATH_SIZE];
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
    strcpy(file_path_no_ext, output_path);
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
  }
  kvs_terminate();
  closedir(dir);

  // wait for child processes to finish
  while (wait(NULL) > 0);

  return 0;
}
