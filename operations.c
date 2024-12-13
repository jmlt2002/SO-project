#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

static void i_sleep() {
    struct timespec delay = (struct timespec){0, 1};
    nanosleep(&delay, NULL);
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  KeyNode **locked_keys = (KeyNode **) calloc(1, sizeof(KeyNode) * num_pairs);
  while (try_lock_keys(kvs_table, num_pairs, keys, locked_keys, 0)) i_sleep();
  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  unlock_keys(num_pairs, locked_keys);
  free(locked_keys);

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  KeyNode **locked_keys = (KeyNode **) calloc(1, sizeof(KeyNode) * num_pairs);
  while (try_lock_keys(kvs_table, num_pairs, keys, locked_keys, 1)) i_sleep();
  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_kvs_key_string);
  write(out_fd, "[", 1);
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      // printf("(%s,KVSERROR)", keys[i]);
      write(out_fd, "(", 1);
      write(out_fd, keys[i], strlen(keys[i]));
      write(out_fd, ",KVSERROR)", 10);
    } else {
      // printf("(%s,%s)", keys[i], result);
      write(out_fd, "(", 1);
      write(out_fd, keys[i], strlen(keys[i]));
      write(out_fd, ",", 1);
      write(out_fd, result, strlen(result));
      write(out_fd, ")", 1);
    }
    free(result);
  }
  write(out_fd, "]\n", 2);
  unlock_keys(num_pairs, locked_keys);
  free(locked_keys);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;
  KeyNode **locked_keys = (KeyNode **) calloc(1, sizeof(KeyNode) * num_pairs);
  while (try_lock_keys(kvs_table, num_pairs, keys, locked_keys, 0)) i_sleep();
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write(out_fd, "[", 1);
        aux = 1;
      }
      // printf("(%s,KVSMISSING)", keys[i]);
      write(out_fd, "(", 1);
      write(out_fd, keys[i], strlen(keys[i]));
      write(out_fd, ",KVSMISSING)", 12);
    }
  }
  if (aux) {
    write(out_fd, "]\n", 2);
  }
  //   unlock_keys(num_pairs, locked_keys); already unlocked on delete pair
  free(locked_keys);
  return 0;
}


void kvs_show(int out_fd, int backup) {
    if (!backup) read_lock_table(kvs_table);
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            // printf("(%s, %s)\n", keyNode->key, keyNode->value);
            write(out_fd, "(", 1);
            write(out_fd, keyNode->key, strlen(keyNode->key));
            write(out_fd, ", ", 2);
            write(out_fd, keyNode->value, strlen(keyNode->value));
            write(out_fd, ")\n", 2);
            keyNode = keyNode->next; // Move to the next node
        }
    }
    if (!backup) read_unlock_table(kvs_table);
}

int kvs_backup(char* backup_path) {
    int bck_fd = open(backup_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    read_lock_table(kvs_table);
    pid_t pid = fork();
    if (pid < 0) return pid;
    if (pid == 0) {
        kvs_show(bck_fd, 1);
        exit(0);
    }

    fsync(bck_fd);  // alternativa a O_SYNC
    close(bck_fd);
    read_unlock_table(kvs_table);
    return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}