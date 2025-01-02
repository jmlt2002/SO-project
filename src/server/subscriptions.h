#include "src/common/constants.h"

// linked list that represents the subscriptions of a certain key
typedef struct InnerNode {
    char notification_pipe[MAX_PIPE_PATH_LENGTH];
    struct InnerNode* next;
} InnerNode;

// linked list of keys
typedef struct OuterNode {
    char* key;
    InnerNode* innerList;
    struct OuterNode* next;
} OuterNode;

InnerNode* createInnerNode(char* pipe_path);

OuterNode* createOuterNode(char* key);

void addToInnerList(InnerNode** head, char* data);

void addToOuterList(OuterNode** head, char* key, InnerNode* innerList);

void addToList(char* key, char* pipe_path);

void removeFromList(char* key, char* pipe_path);

InnerNode* findKey(char* key);

void freeEverything();