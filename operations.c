#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>

#include "kvs.h"
#include "constants.h"

typedef struct {
    struct HashTable* table;
    pthread_rwlock_t rwlock;
} ThreadSafeHashTable;

static ThreadSafeHashTable kvs_store = { NULL, PTHREAD_RWLOCK_INITIALIZER };

extern pthread_t *MAIN_THREADS;
extern DIR *MAIN_DIR;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
    if (kvs_store.table != NULL) {
        fprintf(stderr, "KVS state has already been initialized\n");
        return 1;
    }

    kvs_store.table = create_hash_table();
    if (kvs_store.table == NULL) {
        return 1;
    }

    pthread_rwlock_init(&kvs_store.rwlock, NULL);  // Initialize the read-write lock
    return 0;
}

int kvs_terminate() {
    if (kvs_store.table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    pthread_rwlock_destroy(&kvs_store.rwlock);  // Destroy the read-write lock
    free_table(kvs_store.table);
    kvs_store.table = NULL;
    return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
    if (kvs_store.table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    pthread_rwlock_wrlock(&kvs_store.rwlock);  // Write lock

    for (size_t i = 0; i < num_pairs; i++) {
        if (write_pair(kvs_store.table, keys[i], values[i]) != 0) {
            fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
        }
    }

    pthread_rwlock_unlock(&kvs_store.rwlock);  // Unlock
    return 0;
}


int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
    if (kvs_store.table == NULL) {
      fprintf(stderr, "KVS state must be initialized\n");
      return 1;
    }

    pthread_rwlock_rdlock(&kvs_store.rwlock);  // Read lock

    write(out_fd, "[", 1);
    for (size_t i = 0; i < num_pairs; i++) {
      char* result = read_pair(kvs_store.table, keys[i]);
      if (result == NULL) {
        write(out_fd, "(", 1);
        write(out_fd, keys[i], strlen(keys[i]));
        write(out_fd, ",KVSERROR)", 10);
      } else {
        write(out_fd, "(", 1);
        write(out_fd, keys[i], strlen(keys[i]));
        write(out_fd, ",", 1);
        write(out_fd, result, strlen(result));
        write(out_fd, ")", 1);
      }
      free(result);
    }
    write(out_fd, "]\n", 2);

    pthread_rwlock_unlock(&kvs_store.rwlock);  // Unlock
    return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
    if (kvs_store.table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    pthread_rwlock_wrlock(&kvs_store.rwlock);  // Write lock

    int aux = 0;
    for (size_t i = 0; i < num_pairs; i++) {
      if (delete_pair(kvs_store.table, keys[i]) != 0) {
        if (!aux) {
          write(out_fd, "[", 1);
          aux = 1;
        }
        write(out_fd, "(", 1);
        write(out_fd, keys[i], strlen(keys[i]));
        write(out_fd, ",KVSMISSING)", 12);
      }
    }
    if (aux) {
        write(out_fd, "]\n", 2);
    }

    pthread_rwlock_unlock(&kvs_store.rwlock);  // Unlock
    return 0;
}

void kvs_show(int out_fd) {
  if (kvs_store.table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }

  pthread_rwlock_rdlock(&kvs_store.rwlock);  // Read lock

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode* keyNode = kvs_store.table->table[i];
    while (keyNode != NULL) {
      write(out_fd, "(", 1);
      write(out_fd, keyNode->key, strlen(keyNode->key));
      write(out_fd, ", ", 2);
      write(out_fd, keyNode->value, strlen(keyNode->value));
      write(out_fd, ")\n", 2);
      keyNode = keyNode->next;
    }
  }

  pthread_rwlock_unlock(&kvs_store.rwlock);  // Unlock
}


int kvs_backup(char* backup_path) {
    int bck_fd = open(backup_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    pid_t pid = fork();
    if (pid < 0) return pid;
    if (pid == 0) {
        kvs_show(bck_fd);
        free(MAIN_THREADS);
        closedir(MAIN_DIR);
        exit(0);
    }
    fsync(bck_fd);  // alternativa a O_SYNC
    close(bck_fd);
    
    return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}