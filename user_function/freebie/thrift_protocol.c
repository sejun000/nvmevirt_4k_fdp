#include "gerror.h"
#include "gobject-type.h"
#include "thrift_protocol.h"
#include "thrift_compact_protocol.h"

GType
thrift_protocol_get_type (void)
{
  return G_TYPE_THRIFT_PROTOCOL;
}

gint32
thrift_protocol_write_struct_begin (ThriftProtocol *protocol, const gchar *name,
                                    GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_struct_begin (protocol, name, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_struct_begin (protocol, name, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }

  return -1;
#endif
}

gint32
thrift_protocol_write_struct_end (ThriftProtocol *protocol, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_struct_end (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_struct_end (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_field_begin (ThriftProtocol *protocol,
                                   const gchar *name,
                                   const ThriftType field_type,
                                   const gint16 field_id,
                                   GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_field_begin (protocol, name, field_type,
                                                    field_id, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_field_begin (protocol, name, field_type,
                                                      field_id, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_field_end (ThriftProtocol *protocol, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_field_end (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_field_end (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_field_stop (ThriftProtocol *protocol, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_field_stop (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_field_stop (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_list_begin (ThriftProtocol *protocol,
                                  const ThriftType element_type,
                                  const guint32 size, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_list_begin (protocol, element_type,
                                                   size, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_list_begin (protocol, element_type,
                                                     size, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_list_end (ThriftProtocol *protocol, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_list_end (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_list_end (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_bool (ThriftProtocol *protocol,
                            const gboolean value, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_bool (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_bool (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_byte (ThriftProtocol *protocol, const gint8 value,
                            GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_byte (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_byte (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_i16 (ThriftProtocol *protocol, const gint16 value,
                           GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_i16 (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_i16 (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_i32 (ThriftProtocol *protocol, const gint32 value,
                           GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_i32 (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_i32 (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_i64 (ThriftProtocol *protocol, const gint64 value,
                           GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_i64 (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_i64 (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_string (ThriftProtocol *protocol,
                              const gchar *str, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_string (protocol, str, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_string (protocol, str, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_write_binary (ThriftProtocol *protocol, const gpointer buf,
                              const guint32 len, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_write_binary (protocol, buf, len, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_write_binary (protocol, buf, len, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_struct_begin (ThriftProtocol *protocol,
                                   gchar **name,
                                   GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_struct_begin (protocol, name, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_struct_begin (protocol, name, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_struct_end (ThriftProtocol *protocol, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_struct_end (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_struct_end (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_field_begin (ThriftProtocol *protocol,
                                  gchar **name,
                                  ThriftType *field_type,
                                  gint16 *field_id,
                                  GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_field_begin (protocol, name, field_type,
                                                   field_id, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_field_begin (protocol, name, field_type,
                                                     field_id, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_field_end (ThriftProtocol *protocol,
                                GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_field_end (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_field_end (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_list_begin (ThriftProtocol *protocol,
                                 ThriftType *element_type,
                                 guint32 *size, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_list_begin (protocol, element_type,
                                                  size, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_list_begin (protocol, element_type,
                                                    size, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_list_end (ThriftProtocol *protocol, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_list_end (protocol, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_list_end (protocol, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_bool (ThriftProtocol *protocol, gboolean *value,
                           GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_bool (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_bool (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_byte (ThriftProtocol *protocol, gint8 *value,
                           GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_byte (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_byte (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_i16 (ThriftProtocol *protocol, gint16 *value,
                          GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_i16 (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_i16 (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_i32 (ThriftProtocol *protocol, gint32 *value,
                          GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_i32 (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_i32 (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_i64 (ThriftProtocol *protocol, gint64 *value,
                          GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_i64 (protocol, value, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_i64 (protocol, value, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_string (ThriftProtocol *protocol,
                             gchar **str, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_string (protocol, str, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_string (protocol, str, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

gint32
thrift_protocol_read_binary (ThriftProtocol *protocol, gpointer *buf,
                             guint32 *len, GError **error)
{
#ifdef THRIFT_COMPACT_PROTOCOL_ONLY
  return thrift_compact_protocol_read_binary (protocol, buf, len, error);
#else
  GType protocol_type = G_TYPE_FROM_INSTANCE (protocol);
  switch (protocol_type)
  {
  case G_TYPE_THRIFT_COMPACT_PROTOCOL:
    return thrift_compact_protocol_read_binary (protocol, buf, len, error);
  default:
    g_assert (FALSE, "unhandled protocol");
  }
  return -1;
#endif
}

#define THRIFT_SKIP_RESULT_OR_RETURN(_RES, _CALL) \
  { \
    gint32 _x = (_CALL); \
    if (_x < 0) { return _x; } \
    (_RES) += _x; \
  }

gint32
thrift_protocol_skip (ThriftProtocol *protocol, ThriftType type, GError **error)
{
  switch (type)
  {
    case T_BOOL:
      {
        gboolean boolv;
        return thrift_protocol_read_bool (protocol, &boolv, error);
      }
    case T_BYTE:
      {
        gint8 bytev;
        return thrift_protocol_read_byte (protocol, &bytev, error);
      }

    case T_I16:
      {
        gint16 i16;
        return thrift_protocol_read_i16 (protocol, &i16, error);
      }
    case T_I32:
      {
        gint32 i32;
        return thrift_protocol_read_i32 (protocol, &i32, error);
      }
    case T_I64:
      {
        gint64 i64;
        return thrift_protocol_read_i64 (protocol, &i64, error);
      }
    case T_STRING:
      {
        gpointer data;
        guint32 len;
        gint32 ret = thrift_protocol_read_binary (protocol, &data, &len, error);
        return ret;
      }
    case T_STRUCT:
      {
        gint32 result = 0;
        gchar *name;
        gint16 fid;
        ThriftType ftype;
        THRIFT_SKIP_RESULT_OR_RETURN (result,
          thrift_protocol_read_struct_begin (protocol, &name, error))
        while (1)
        {
          THRIFT_SKIP_RESULT_OR_RETURN (result,
            thrift_protocol_read_field_begin (protocol, &name, &ftype,
                                              &fid, error))
          if (ftype == T_STOP)
          {
            break;
          }
          THRIFT_SKIP_RESULT_OR_RETURN (result,
            thrift_protocol_skip (protocol, ftype, error))
          THRIFT_SKIP_RESULT_OR_RETURN (result,
            thrift_protocol_read_field_end (protocol, error))
        }
        THRIFT_SKIP_RESULT_OR_RETURN (result,
          thrift_protocol_read_struct_end (protocol, error))
        return result;
      }
    case T_LIST:
      {
        gint32 result = 0;
        ThriftType elem_type;
        guint32 i, size;
        THRIFT_SKIP_RESULT_OR_RETURN (result,
          thrift_protocol_read_list_begin (protocol, &elem_type, &size,
                                           error))
        for (i = 0; i < size; i++)
        {
          THRIFT_SKIP_RESULT_OR_RETURN (result,
          thrift_protocol_skip (protocol, elem_type, error))
        }
        THRIFT_SKIP_RESULT_OR_RETURN (result,
          thrift_protocol_read_list_end (protocol, error))
        return result;
      }
    default:
      break;
  }

  g_set_error (error, THRIFT_PROTOCOL_ERROR,
               THRIFT_PROTOCOL_ERROR_INVALID_DATA,
               "unrecognized type");
  return -1;
}

/* define the GError domain for Thrift protocols */
GQuark
thrift_protocol_error_quark (void)
{
  return G_QUARK_THRIFT_PROTOCOL_ERROR;
}

void
thrift_protocol_init (ThriftProtocol *protocol)
{
  protocol->transport = NULL;
}

void
thrift_protocol_dispose (GObject *gobject)
{
  ThriftProtocol *tp;

  tp = THRIFT_PROTOCOL (gobject);
  g_object_unref (tp->transport);
}
