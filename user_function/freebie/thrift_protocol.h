#ifndef THRIFT_PROTOCOL_H
#define THRIFT_PROTOCOL_H

#include "gobject.h"
#include "gerror.h"
#include "thrift_transport.h"

/* Use thrift compact protocol only */
#define THRIFT_COMPACT_PROTOCOL_ONLY

/**
 * Enumerated definition of the types that the Thrift protocol supports.
 * Take special note of the T_END type which is used specifically to mark
 * the end of a sequence of fields.
 */
typedef enum {
  T_STOP   = 0,
  T_VOID   = 1,
  T_BOOL   = 2,
  T_BYTE   = 3,
  T_I08    = 3,
  T_I16    = 6,
  T_I32    = 8,
  T_U64    = 9,
  T_I64    = 10,
  T_DOUBLE = 4,
  T_STRING = 11,
  T_UTF7   = 11,
  T_STRUCT = 12,
  T_MAP    = 13,
  T_SET    = 14,
  T_LIST   = 15,
  T_UTF8   = 16,
  T_UTF16  = 17
} ThriftType;

/* type macros */
#define THRIFT_TYPE_PROTOCOL (thrift_protocol_get_type ())
#define THRIFT_PROTOCOL(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_PROTOCOL, ThriftProtocol))
#define THRIFT_IS_PROTOCOL(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj), THRIFT_TYPE_PROTOCOL))

/*!
 * ThriftProtocol instance
 */
typedef struct _ThriftProtocol ThriftProtocol;
struct _ThriftProtocol
{
  GObject parent;

  ThriftTransport *transport;
};

/* used by THRIFT_TYPE_PROTOCOL */
GType thrift_protocol_get_type (void);

gint32 thrift_protocol_write_struct_begin (ThriftProtocol *protocol,
                                           const gchar *name,
                                           GError **error);

gint32 thrift_protocol_write_struct_end (ThriftProtocol *protocol,
                                         GError **error);

gint32 thrift_protocol_write_field_begin (ThriftProtocol *protocol,
                                          const gchar *name,
                                          const ThriftType field_type,
                                          const gint16 field_id,
                                          GError **error);

gint32 thrift_protocol_write_field_end (ThriftProtocol *protocol,
                                        GError **error);

gint32 thrift_protocol_write_field_stop (ThriftProtocol *protocol,
                                         GError **error);

gint32 thrift_protocol_write_list_begin (ThriftProtocol *protocol,
                                         const ThriftType element_type,
                                         const guint32 size, GError **error);

gint32 thrift_protocol_write_list_end (ThriftProtocol *protocol,
                                       GError **error);

gint32 thrift_protocol_write_bool (ThriftProtocol *protocol,
                                   const gboolean value, GError **error);

gint32 thrift_protocol_write_byte (ThriftProtocol *protocol, const gint8 value,
                                   GError **error);

gint32 thrift_protocol_write_i16 (ThriftProtocol *protocol, const gint16 value,
                                  GError **error);

gint32 thrift_protocol_write_i32 (ThriftProtocol *protocol, const gint32 value,
                                  GError **error);

gint32 thrift_protocol_write_i64 (ThriftProtocol *protocol, const gint64 value,
                                  GError **error);

gint32 thrift_protocol_write_string (ThriftProtocol *protocol,
                                     const gchar *str, GError **error);

gint32 thrift_protocol_write_binary (ThriftProtocol *protocol,
                                     const gpointer buf,
                                     const guint32 len, GError **error);

gint32 thrift_protocol_read_struct_begin (ThriftProtocol *protocol,
                                          gchar **name,
                                          GError **error);

gint32 thrift_protocol_read_struct_end (ThriftProtocol *protocol,
                                        GError **error);

gint32 thrift_protocol_read_field_begin (ThriftProtocol *protocol,
                                         gchar **name,
                                         ThriftType *field_type,
                                         gint16 *field_id,
                                         GError **error);

gint32 thrift_protocol_read_field_end (ThriftProtocol *protocol,
                                       GError **error);

gint32 thrift_protocol_read_list_begin (ThriftProtocol *protocol,
                                        ThriftType *element_type,
                                        guint32 *size, GError **error);

gint32 thrift_protocol_read_list_end (ThriftProtocol *protocol, GError **error);

gint32 thrift_protocol_read_bool (ThriftProtocol *protocol, gboolean *value,
                                  GError **error);

gint32 thrift_protocol_read_byte (ThriftProtocol *protocol, gint8 *value,
                                  GError **error);

gint32 thrift_protocol_read_i16 (ThriftProtocol *protocol, gint16 *value,
                                 GError **error);

gint32 thrift_protocol_read_i32 (ThriftProtocol *protocol, gint32 *value,
                                 GError **error);

gint32 thrift_protocol_read_i64 (ThriftProtocol *protocol, gint64 *value,
                                 GError **error);

gint32 thrift_protocol_read_string (ThriftProtocol *protocol,
                                    gchar **str, GError **error);

gint32 thrift_protocol_read_binary (ThriftProtocol *protocol,
                                    gpointer *buf, guint32 *len,
                                    GError **error);

gint32 thrift_protocol_skip (ThriftProtocol *protocol, ThriftType type,
                             GError **error);

void thrift_protocol_init (ThriftProtocol *self);

void thrift_protocol_dispose (GObject *gobject);

/* define error types */
typedef enum
{
  THRIFT_PROTOCOL_ERROR_UNKNOWN,
  THRIFT_PROTOCOL_ERROR_INVALID_DATA,
  THRIFT_PROTOCOL_ERROR_NEGATIVE_SIZE,
  THRIFT_PROTOCOL_ERROR_SIZE_LIMIT,
  THRIFT_PROTOCOL_ERROR_BAD_VERSION,
  THRIFT_PROTOCOL_ERROR_NOT_IMPLEMENTED,
  THRIFT_PROTOCOL_ERROR_DEPTH_LIMIT
} ThriftProtocolError;

/* define an error domain for GError to use */
#define THRIFT_PROTOCOL_ERROR (thrift_protocol_error_quark ())
GQuark thrift_protocol_error_quark (void);

#endif /* THRIFT_PROTOCOL_H */
