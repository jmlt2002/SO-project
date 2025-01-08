#ifndef PCBUFFER_H
#define PCBUFFER_H

#include "src/common/constants.h"

typedef struct {
    char* request_pipe;
    char* response_pipe;
    char* notification_pipe;
} BufferData;

typedef struct BufferNode {
    BufferData data;
    struct BufferNode* next;
} BufferNode;

typedef struct {
    BufferNode* front;
    BufferNode* rear;
} Buffer;

void initBuffer(Buffer* q);
int isBufferEmpty(Buffer* q);
void insertInBuffer(Buffer* q, BufferData data);
BufferData removeFromBuffer(Buffer* q);

// TODO: cleanup function

#endif
