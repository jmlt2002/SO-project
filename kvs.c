#include "kvs.h"
#include "string.h"
#include "constants.h"

#include <stdlib.h>
#include <ctype.h>

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}


struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
  }
  return ht;
}

int compare_kvs_key_string(const void *p1, const void *p2) {
    return strcmp((const char *) p1, (const char *) p2);
}

static int find_key_node(size_t n, KeyNode **where, char *what) {
    if (!where || !what) return 0;
    for (int i=0; i< (int)n; i++) {
        if (where[i] && !strcmp(where[i]->key, what)) return 1;
    }
    return 0;
}

int try_lock_keys(HashTable *ht, size_t num_keys, char keys[][MAX_STRING_SIZE], KeyNode **dest_locked_nodes) {
    int fail = 0;
    int i;
    char *keys_dup = (char *) malloc(sizeof(char) * MAX_STRING_SIZE * num_keys);
    memcpy(keys_dup, keys, sizeof(char) * MAX_STRING_SIZE * num_keys);

    qsort(keys_dup, num_keys, MAX_STRING_SIZE, compare_kvs_key_string); // force sort keys in order to lock

    for (i=0; i < (int)num_keys; i++) {
        if (find_key_node(num_keys, dest_locked_nodes, keys[i])) continue;
        KeyNode *keyNode = ht->table[hash(keys[i])];
        // Search for the key node
        while (keyNode != NULL) {
            if (strcmp(keyNode->key, &keys_dup[i*MAX_STRING_SIZE]) == 0) {
                if (!pthread_rwlock_trywrlock(&keyNode->rw_mtx)) {
                    dest_locked_nodes[i] = keyNode;
                } else {
                    fail = 1;
                    break;
                }
            }
            keyNode = keyNode->next; // Move to the next node
        }
    }

    if (fail) {
        for (i=0; i < (int)num_keys; i++) {
            if (dest_locked_nodes[i]) pthread_rwlock_unlock(&dest_locked_nodes[i]->rw_mtx);
        }
        free(keys_dup);
        return -1;
    }
    free(keys_dup);
    return 0;
}

void unlock_keys(size_t num_keys, KeyNode **locked_nodes) {
    for (int i=0; i < (int)num_keys; i++)
        if (locked_nodes[i]) pthread_rwlock_unlock(&locked_nodes[i]->rw_mtx);
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            free(keyNode->value);
            keyNode->value = strdup(value);
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    pthread_rwlock_init(&keyNode->rw_mtx, NULL);
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        pthread_rwlock_tryrdlock(&keyNode->rw_mtx);
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            pthread_rwlock_unlock(&keyNode->rw_mtx);
            return value; // Return copy of the value if found
        }
        pthread_rwlock_unlock(&keyNode->rw_mtx);
        keyNode = keyNode->next; // Move to the next node
    }
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    // Search for the key node
    while (keyNode != NULL) {
        pthread_rwlock_trywrlock(&keyNode->rw_mtx);
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            pthread_rwlock_unlock(&keyNode->rw_mtx);
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }

        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node

    }
    
    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    free(ht);
}