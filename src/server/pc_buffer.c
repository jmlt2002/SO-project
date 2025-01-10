// linked-list that will work as a PCBuffer

#include "pc_buffer.h"

#include <stdio.h>
#include <stdlib.h>

void initBuffer(Buffer* b) {
    b->front = NULL;
    b->rear = NULL;
}

int isBufferEmpty(Buffer* b) {
    return b->front == NULL;
}

void insertInBuffer(Buffer* b, BufferData data) {
    BufferNode* newNode = (BufferNode*)malloc(sizeof(BufferNode));
    if (newNode == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(1);
    }
    newNode->data = data;
    newNode->next = NULL;

    if (b->rear == NULL) {
        b->front = b->rear = newNode;
    } else {
        b->rear->next = newNode;
        b->rear = newNode;
    }
}

BufferData removeFromBuffer(Buffer* b) {
    if (isBufferEmpty(b)) {
        return (BufferData) {{0}, {0}, {0}};
    }

    BufferNode* temp = b->front;
    BufferData value = temp->data;
    b->front = b->front->next;

    if (b->front == NULL) {
        b->rear = NULL;
    }

    free(temp);
    return value;
}

void destroyBuffer(Buffer* b) {
    while (!isBufferEmpty(b)) {
        removeFromBuffer(b);
    }
}
