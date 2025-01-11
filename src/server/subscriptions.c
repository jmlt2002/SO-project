#include "subscriptions.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

OuterNode* subscriptions_head = NULL;

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
    OuterNode* current = subscriptions_head;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            addToInnerList(&current->innerList, notification_pipe);
            return;
        }
        current = current->next;
    }
    InnerNode* innerList = createInnerNode(notification_pipe);
    addToOuterList(&subscriptions_head, key, innerList);
}

void removeFromList(char* key, int notification_pipe) {
    OuterNode* current = subscriptions_head;
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
                    close(innerCurrent->notification_pipe); // Close the file descriptor
                    free(innerCurrent);
                    return;
                }
                innerPrev = innerCurrent;
                innerCurrent = innerCurrent->next;
            }
        }
        current = current->next;
    }
}

void removeKey(char* key) {
    OuterNode* current = subscriptions_head;
    OuterNode* prev = NULL;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            if (prev == NULL) {
                subscriptions_head = current->next;
            } else {
                prev->next = current->next;
            }
            free(current->key);
            InnerNode* innerCurrent = current->innerList;
            while (innerCurrent != NULL) {
                InnerNode* temp = innerCurrent;
                innerCurrent = innerCurrent->next;
                close(temp->notification_pipe); // Close the file descriptor
                free(temp);
            }
            free(current);
            return;
        }
        prev = current;
        current = current->next;
    }
}

InnerNode* findKey(char* key) {
    OuterNode* current = subscriptions_head;
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            return current->innerList;
        }
        current = current->next;
    }
    return NULL;
}

void cleanupSubscriptions() {
    OuterNode* current = subscriptions_head;
    while (current != NULL) {
        InnerNode* innerCurrent = current->innerList;
        while (innerCurrent != NULL) {
            InnerNode* temp = innerCurrent;
            innerCurrent = innerCurrent->next;
            close(temp->notification_pipe); // Close the file descriptor
            free(temp);
        }
        OuterNode* temp = current;
        current = current->next;
        free(temp->key); // Free the dynamically allocated key
        free(temp);
    }
}
