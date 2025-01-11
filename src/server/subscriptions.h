#ifndef SUBSCRIPTIONS_H
#define SUBSCRIPTIONS_H

#include "src/common/constants.h"

// linked list that represents the subscriptions of a certain key
typedef struct InnerNode {
    int notification_pipe;
    struct InnerNode* next;
} InnerNode;

// linked list of keys
typedef struct OuterNode {
    char* key;
    InnerNode* innerList;
    struct OuterNode* next;
} OuterNode;

InnerNode* createInnerNode(int notification_pipe);

OuterNode* createOuterNode(char* key);

void addToInnerList(InnerNode** head, int notification_pipe);

void addToOuterList(OuterNode** head, char* key, InnerNode* innerList);

void addToList(char* key, int notification_pipe);

void removeFromList(char* key, int notification_pipe);

void removeKey(char* key);

InnerNode* findKey(char* key);

void cleanupSubscriptions();

#endif