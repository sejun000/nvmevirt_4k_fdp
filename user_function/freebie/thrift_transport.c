#include "gobject-type.h"
#include "thrift.h"
#include "thrift_transport.h"
#include "thrift_file_transport.h"

/* Public */

GType
thrift_transport_get_type (void)
{
  return G_TYPE_THRIFT_TRANSPORT;
}

gboolean
thrift_transport_open (ThriftTransport *transport, GError **error)
{
#ifdef THRIFT_FILE_TRANSPORT_ONLY
  return thrift_file_transport_open (transport, error);
#else
  GType transport_type = G_TYPE_FROM_INSTANCE (transport);
  switch (transport_type)
  {
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    return thrift_file_transport_open (transport, error);
  default:
    g_assert (FALSE, "unhandled transport type");
  }
  return FALSE;
#endif
}

gboolean
thrift_transport_close (ThriftTransport *transport, GError **error)
{
#ifdef THRIFT_FILE_TRANSPORT_ONLY
  return thrift_file_transport_close (transport, error);
#else
  GType transport_type = G_TYPE_FROM_INSTANCE (transport);
  switch (transport_type)
  {
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    return thrift_file_transport_close (transport, error);
  default:
    g_assert (FALSE, "unhandled transport type");
  }
  return FALSE;
#endif
}

gboolean
thrift_transport_write (ThriftTransport *transport, const gpointer buf,
                        const guint32 len, GError **error)
{
#ifdef THRIFT_FILE_TRANSPORT_ONLY
  return thrift_file_transport_write (transport, buf, len, error);
#else
  GType transport_type = G_TYPE_FROM_INSTANCE (transport);
  switch (transport_type)
  {
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    return thrift_file_transport_write (transport, buf, len, error);
  default:
    g_assert (FALSE, "unhandled transport type");
  }
  return FALSE;
#endif
}

gboolean
thrift_transport_flush (ThriftTransport *transport, GError **error)
{
#ifdef THRIFT_FILE_TRANSPORT_ONLY
  return thrift_file_transport_flush (transport, error);
#else
  GType transport_type = G_TYPE_FROM_INSTANCE (transport);
  switch (transport_type)
  {
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    return thrift_file_transport_flush (transport, error);
  default:
    g_assert (FALSE, "unhandled transport type");
  }
  return FALSE;
#endif
}

gint32
thrift_transport_read (ThriftTransport *transport, gpointer buf, guint32 len,
                       GError **error)
{
#ifdef THRIFT_FILE_TRANSPORT_ONLY
  return thrift_file_transport_read (transport, buf, len, error);
#else
  GType transport_type = G_TYPE_FROM_INSTANCE (transport);
  switch (transport_type)
  {
  case G_TYPE_THRIFT_FILE_TRANSPORT:
    return thrift_file_transport_read (transport, buf, len, error);
  default:
    g_assert (FALSE, "unhandled transport type");
  }
  return -1;
#endif
}

void
thrift_transport_init (ThriftTransport *transport)
{
  THRIFT_UNUSED_VAR (transport);
}

void
thrift_transport_dispose (GObject *gobject)
{
  THRIFT_UNUSED_VAR (gobject);
}
