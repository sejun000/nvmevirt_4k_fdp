/*
 * `thrift_compact_protocol.h` is imported from the official codebase.
 * The only changes in the file is deletion of unused functions.
 * @see https://github.com/apache/thrift/blob/v0.21.0/lib/c_glib/src/thrift/c_glib/protocol/thrift_compact_protocol.h
 */
#ifndef THRIFT_COMPACT_PROTOCOL_H
#define THRIFT_COMPACT_PROTOCOL_H

#include "thrift_protocol.h"
#include "gqueue.h"

/* type macros */
#define THRIFT_TYPE_COMPACT_PROTOCOL (thrift_compact_protocol_get_type ())
#define THRIFT_COMPACT_PROTOCOL(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_COMPACT_PROTOCOL, \
   ThriftCompactProtocol))
#define THRIFT_IS_COMPACT_PROTOCOL(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), THRIFT_TYPE_COMPACT_PROTOCOL))

/*!
 * Thrift Compact Protocol instance.
 */
typedef struct _ThriftCompactProtocol ThriftCompactProtocol;
struct _ThriftCompactProtocol
{
  ThriftProtocol parent;

  /* protected */
  gint32 string_limit;
  gint32 container_limit;

  /* private */

  /**
   * (Writing) If we encounter a boolean field begin, save the TField here
   * so it can have the value incorporated.
   */
  const gchar* _bool_field_name;
  ThriftType _bool_field_type;
  gint16 _bool_field_id;

  /**
   * (Reading) If we read a field header, and it's a boolean field, save
   * the boolean value here so that read_bool can use it.
   */
  gboolean _has_bool_value;
  gboolean _bool_value;

  /**
   * Used to keep track of the last field for the current and previous structs,
   * so we can do the delta stuff.
   */

  GQueue _last_field;
  gint16 _last_field_id;
};

/* used by THRIFT_TYPE_COMPACT_PROTOCOL */
GType thrift_compact_protocol_get_type (void);

gint32 thrift_compact_protocol_write_struct_begin (ThriftProtocol *protocol,
                                                   const gchar *name,
                                                   GError **error);

gint32 thrift_compact_protocol_write_struct_end (ThriftProtocol *protocol,
                                                 GError **error);

gint32 thrift_compact_protocol_write_field_begin (ThriftProtocol *protocol,
                                                  const gchar *name,
                                                  const ThriftType field_type,
                                                  const gint16 field_id,
                                                  GError **error);

gint32 thrift_compact_protocol_write_field_end (ThriftProtocol *protocol,
                                                GError **error);

gint32 thrift_compact_protocol_write_field_stop (ThriftProtocol *protocol,
                                                 GError **error);

gint32 thrift_compact_protocol_write_list_begin (ThriftProtocol *protocol,
                                                 const ThriftType element_type,
                                                 const guint32 size,
                                                 GError **error);

gint32 thrift_compact_protocol_write_list_end (ThriftProtocol *protocol,
                                               GError **error);

gint32 thrift_compact_protocol_write_bool (ThriftProtocol *protocol,
                                           const gboolean value,
                                           GError **error);

gint32 thrift_compact_protocol_write_byte (ThriftProtocol *protocol,
                                           const gint8 value, GError **error);

gint32 thrift_compact_protocol_write_i16 (ThriftProtocol *protocol,
                                          const gint16 value, GError **error);

gint32 thrift_compact_protocol_write_i32 (ThriftProtocol *protocol,
                                          const gint32 value, GError **error);

gint32 thrift_compact_protocol_write_i64 (ThriftProtocol *protocol,
                                          const gint64 value, GError **error);

gint32 thrift_compact_protocol_write_string (ThriftProtocol *protocol,
                                             const gchar *str, GError **error);

gint32 thrift_compact_protocol_write_binary (ThriftProtocol *protocol,
                                             const gpointer buf,
                                             const guint32 len, GError **error);

gint32 thrift_compact_protocol_read_struct_begin (ThriftProtocol *protocol,
                                                  gchar **name, GError **error);

gint32 thrift_compact_protocol_read_struct_end (ThriftProtocol *protocol,
                                                GError **error);

gint32 thrift_compact_protocol_read_field_begin (ThriftProtocol *protocol,
                                                 gchar **name,
                                                 ThriftType *field_type,
                                                 gint16 *field_id,
                                                 GError **error);

gint32 thrift_compact_protocol_read_field_end (ThriftProtocol *protocol,
                                               GError **error);

gint32 thrift_compact_protocol_read_list_begin (ThriftProtocol *protocol,
                                                ThriftType *element_type,
                                                guint32 *size, GError **error);

gint32 thrift_compact_protocol_read_list_end (ThriftProtocol *protocol,
                                              GError **error);

gint32 thrift_compact_protocol_read_bool (ThriftProtocol *protocol,
                                          gboolean *value, GError **error);

gint32 thrift_compact_protocol_read_byte (ThriftProtocol *protocol, gint8 *value,
                                          GError **error);

gint32 thrift_compact_protocol_read_i16 (ThriftProtocol *protocol, gint16 *value,
                                         GError **error);

gint32 thrift_compact_protocol_read_i32 (ThriftProtocol *protocol, gint32 *value,
                                         GError **error);

gint32 thrift_compact_protocol_read_i64 (ThriftProtocol *protocol, gint64 *value,
                                         GError **error);

gint32 thrift_compact_protocol_read_string (ThriftProtocol *protocol,
                                            gchar **str, GError **error);

gint32 thrift_compact_protocol_read_binary (ThriftProtocol *protocol,
                                            gpointer *buf, guint32 *len,
                                            GError **error);

void thrift_compact_protocol_init (ThriftCompactProtocol *self);

void thrift_compact_protocol_dispose (GObject *gobject);

#endif /* THRIFT_COMPACT_PROTOCOL_H */
