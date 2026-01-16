#ifndef THRIFT_TRANSPORT_H
#define THRIFT_TRANSPORT_H

#include "gtypes.h"
#include "gobject.h"
#include "gerror.h"
#include "thrift.h"

/* Use thrift file transport only */
#define THRIFT_FILE_TRANSPORT_ONLY

#define THRIFT_TYPE_TRANSPORT (thrift_transport_get_type ())
#define THRIFT_TRANSPORT(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_TRANSPORT, ThriftTransport))
#define THRIFT_IS_TRANSPORT(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), THRIFT_TYPE_TRANSPORT))

/*!
 * Thrift Protocol object
 */
typedef struct _ThriftTransport ThriftTransport;
struct _ThriftTransport
{
  GObject parent;
};

/* used by THRIFT_TYPE_TRANSPORT */
GType thrift_transport_get_type (void);

/* virtual public methods */
gboolean thrift_transport_open (ThriftTransport *transport, GError **error);

gboolean thrift_transport_close (ThriftTransport *transport, GError **error);

gboolean thrift_transport_write (ThriftTransport *transport, const gpointer buf,
                                 const guint32 len, GError **error);

gboolean thrift_transport_flush (ThriftTransport *transport, GError **error);

gint32 thrift_transport_read (ThriftTransport *transport, gpointer buf,
                              guint32 len, GError **error);

static inline
gint32 thrift_transport_read_all (ThriftTransport *transport, gpointer buf,
                                  guint32 len, GError **error)
{
  guint32 have;
  gint32 ret;
  gint8 *bytes;

  THRIFT_UNUSED_VAR (error);

  have = 0;
  ret = 0;
  bytes = (gint8*) buf;

  while (have < len)
  {
    if ((ret = thrift_transport_read (transport, (gpointer) (bytes + have),
                                      len - have, error)) < 0)
    {
      return ret;
    }
    have += ret;
  }

  return have;
}

void thrift_transport_init (ThriftTransport *transport);

void thrift_transport_dispose (GObject *gobject);

/* define error/exception types */
typedef enum
{
  THRIFT_TRANSPORT_ERROR_UNKNOWN,
  THRIFT_TRANSPORT_ERROR_HOST,
  THRIFT_TRANSPORT_ERROR_SOCKET,
  THRIFT_TRANSPORT_ERROR_CONNECT,
  THRIFT_TRANSPORT_ERROR_SEND,
  THRIFT_TRANSPORT_ERROR_RECEIVE,
  THRIFT_TRANSPORT_ERROR_CLOSE,
  THRIFT_TRANSPORT_ERROR_MAX_MESSAGE_SIZE_REACHED
} ThriftTransportError;

/* define an error domain for GError to use */
#define THRIFT_TRANSPORT_ERROR (thrift_transport_error_quark ())
static inline GQuark
thrift_transport_error_quark (void)
{
  return G_QUARK_THRIFT_TRANSPORT_ERROR;
}

#endif /* THRIFT_TRANSPORT_H */
