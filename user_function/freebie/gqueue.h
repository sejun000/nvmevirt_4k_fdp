#ifndef G_QUEUE_H
#define G_QUEUE_H

#include "gtypes.h"

/*
 * XXX:
 * While reading the Parquet file, the maximum queue length is 5.  So, we fixed
 * the queue size to 8 to simplify the implementation.
 */
#define G_QUEUE_MAXLEN 8

typedef struct _GQueue GQueue;
struct _GQueue
{
  gpointer data[G_QUEUE_MAXLEN];
  guint8 tail;
};

static inline void
g_queue_push_tail (GQueue *queue, gpointer data)
{
  queue->data[queue->tail++] = data;
}

static inline gpointer
g_queue_pop_tail (GQueue *queue)
{
  return queue->data[--queue->tail];
}

static inline void
g_queue_init (GQueue *queue)
{
  queue->tail = 0;
}

#endif /* G_QUEUE_H */
