#include "subscriptions.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

typedef struct {
    OuterNode* node;
    pthread_mutex_t mutex;
} SubscriptionsHead;

SubscriptionsHead subscriptions_head = {NULL, PTHREAD_MUTEX_INITIALIZER};

InnerNode* createInnerNode(int notification_pipe) {
    InnerNode* newNode = (InnerNode*)malloc(sizeof(InnerNode));
    if (!newNode) {
        perror("Failed to allocate memory for inner node");
        exit(EXIT_FAILURE);
    }
    newNode->notification_pipe = notification_pipe;
    newNode->next = NULL;
    return newNode;
}

OuterNode* createOuterNode(char* key) {
    OuterNode* newNode = (OuterNode*)malloc(sizeof(OuterNode));
    if (!newNode) {
        perror("Failed to allocate memory for outer node");
        exit(EXIT_FAILURE);
    }
    newNode->key = strdup(key);
    if (!newNode->key) {
        perror("Failed to allocate memory for key");
        free(newNode);
        exit(EXIT_FAILURE);
    }
    newNode->innerList = NULL;
    newNode->next = NULL;
    return newNode;
}

void addToInnerList(InnerNode** head, int notification_pipe) {
    InnerNode* newNode = createInnerNode(notification_pipe);
    newNode->next = *head;
    *head = newNode;
}

void addToOuterList(OuterNode** head, char* key, InnerNode* innerList) {
    OuterNode* newNode = createOuterNode(key);
    newNode->innerList = innerList;
    newNode->next = *head;
    *head = newNode;
}

void addToList(char* key, int notification_pipe) {
    pthread_mutex_lock(&subscriptions_head.mutex);
    OuterNode* current = subscriptions_head.node;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            addToInnerList(&current->innerList, notification_pipe);
            pthread_mutex_unlock(&subscriptions_head.mutex);
            return;
        }
        current = current->next;
    }
    InnerNode* innerList = createInnerNode(notification_pipe);
    addToOuterList(&subscriptions_head.node, key, innerList);
    pthread_mutex_unlock(&subscriptions_head.mutex);
}

void removeFromList(char* key, int notification_pipe) {
    pthread_mutex_lock(&subscriptions_head.mutex);
    OuterNode* current = subscriptions_head.node;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            InnerNode* innerCurrent = current->innerList;
            InnerNode* innerPrev = NULL;
            while (innerCurrent != NULL) {
                if (innerCurrent->notification_pipe == notification_pipe) {
                    if (innerPrev == NULL) {
                        current->innerList = innerCurrent->next;
                    } else {
                        innerPrev->next = innerCurrent->next;
                    }
                    free(innerCurrent);
                    pthread_mutex_unlock(&subscriptions_head.mutex);
                    return;
                }
                innerPrev = innerCurrent;
                innerCurrent = innerCurrent->next;
            }
        }
        current = current->next;
    }
    pthread_mutex_unlock(&subscriptions_head.mutex);
}

void removeKey(char* key) {
    pthread_mutex_lock(&subscriptions_head.mutex);
    OuterNode* current = subscriptions_head.node;
    OuterNode* prev = NULL;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            if (prev == NULL) {
                subscriptions_head.node = current->next;
            } else {
                prev->next = current->next;
            }
            free(current->key);
            InnerNode* innerCurrent = current->innerList;
            while (innerCurrent != NULL) {
                InnerNode* temp = innerCurrent;
                innerCurrent = innerCurrent->next;
                free(temp);
            }
            free(current);
            pthread_mutex_unlock(&subscriptions_head.mutex);
            return;
        }
        prev = current;
        current = current->next;
    }
    pthread_mutex_unlock(&subscriptions_head.mutex);
}

InnerNode* copyInnerList(InnerNode* head) {
    InnerNode* newHead = NULL;
    InnerNode* current = head;
    InnerNode* prev = NULL;
    while (current != NULL) {
        InnerNode* newNode = createInnerNode(current->notification_pipe);
        if (prev == NULL) {
            newHead = newNode;
        } else {
            prev->next = newNode;
        }
        prev = newNode;
        current = current->next;
    }
    return newHead;
}

InnerNode* findKey(char* key) {
    pthread_mutex_lock(&subscriptions_head.mutex);
    OuterNode* current = subscriptions_head.node;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            // return copy of the inner list
            InnerNode* copy = copyInnerList(current->innerList);
            pthread_mutex_unlock(&subscriptions_head.mutex);
            return copy;
        }
        current = current->next;
    }
    pthread_mutex_unlock(&subscriptions_head.mutex);
    return NULL;
}

void freeInnerList(InnerNode* head) {
    InnerNode* current = head;
    while (current != NULL) {
        InnerNode* temp = current;
        current = current->next;
        free(temp);
    }
}

void cleanupSubscriptions() {
    pthread_mutex_lock(&subscriptions_head.mutex);
    OuterNode* current = subscriptions_head.node;
    while (current != NULL) {
        InnerNode* innerCurrent = current->innerList;
        while (innerCurrent != NULL) {
            InnerNode* temp = innerCurrent;
            innerCurrent = innerCurrent->next;
            free(temp);
        }
        OuterNode* temp = current;
        current = current->next;
        free(temp->key);
        free(temp);
    }
    subscriptions_head.node = NULL;
    pthread_mutex_unlock(&subscriptions_head.mutex);
}
