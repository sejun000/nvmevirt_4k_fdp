#ifndef THRIFT_FILE_TRANSPORT_H
#define THRIFT_FILE_TRANSPORT_H

#include "gobject-type.h"
#include "gio.h"
#include "gtypes.h"
#include "garray.h"
#include "thrift_transport.h"

/*! \file thrift_file_transport.h
 *  \brief Class for Thrift file descriptor transports.
 */

/* type macros */
#define THRIFT_TYPE_FILE_TRANSPORT (thrift_file_transport_get_type ())
#define THRIFT_FILE_TRANSPORT(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_FILE_TRANSPORT, \
                               ThriftFileTransport))
#define THRIFT_IS_FILE_TRANSPORT(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), THRIFT_TYPE_FILE_TRANSPORT))

/*!
 * ThriftFileTransport instance
 */
typedef struct _ThriftFileTransport ThriftFileTransport;
struct _ThriftFileTransport
{
  ThriftTransport parent;

  // FD fd;
  char *filename;
  gssize file_size;

  goffset location;
  goffset max_file_size;

  GByteArray *r_buffer;
  goffset r_buffer_location;
};

/* used by THRIFT_TYPE_FILE_TRANSPORT */
GType thrift_file_transport_get_type (void);

gboolean thrift_file_transport_open (ThriftTransport *transport, GError **error);

gboolean thrift_file_transport_close (ThriftTransport *transport, GError **error);

gint32 thrift_file_transport_read (ThriftTransport *transport, gpointer buf,
                                   guint32 len, GError **error);

gboolean thrift_file_transport_write (ThriftTransport *transport,
                                      const gpointer buf, const guint32 len,
                                      GError **error);

gboolean thrift_file_transport_flush (ThriftTransport *transport, GError **error);

void thrift_file_transport_init (ThriftFileTransport *transport);

void thrift_file_transport_finalize (GObject *object);

gboolean thrift_file_transport_prefetch (ThriftTransport *transport, goffset pos,
                                         gsize len, GError **error);

goffset thrift_file_transport_get_location (ThriftTransport *transport);

void thrift_file_transport_set_location (ThriftTransport *transport,
                                         goffset location);

gssize thrift_file_transport_get_size (ThriftTransport *transport,
                                       GError **error);

void print_file_transport_time(void);
/*
 * Return ptr for the r_buffer
 * XXX: This func is 'unsafe' as the memory address for the ptr is not
 *      guaranteed to be safe.
 */
static inline
gpointer thrift_file_transport_get_unsafe_ptr (ThriftTransport *transport,
                                               guint32 len)
{
  ThriftFileTransport *t;
  gpointer ptr;

  t = THRIFT_FILE_TRANSPORT (transport);

  /* Out of r_buffer boundary */
  g_assert (t->r_buffer != NULL ||
            t->location + len <= t->r_buffer_location + t->r_buffer->len,
            "data not prefetched");

  ptr = t->r_buffer->data + t->location - t->r_buffer_location;
  t->location += len;

  return ptr;
}

#endif /* THRIFT_FILE_TRANSPORT_H */
