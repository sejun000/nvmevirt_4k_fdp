#ifndef THRIFT_STRUCT_H
#define THRIFT_STRUCT_H

#include "gobject-type.h"
#include "gobject.h"
#include "gerror.h"
#include "thrift_protocol.h"

#define THRIFT_STRUCT(obj) \
    (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_STRUCT, ThriftStruct))

/* struct */
typedef struct _ThriftStruct ThriftStruct;
struct _ThriftStruct
{
  GObject parent;
};

gint32 thrift_struct_read (ThriftStruct *object, ThriftProtocol *protocol,
                           GError **error);

gint32 thrift_struct_write (ThriftStruct *object, ThriftProtocol *protocol,
                            GError **error);
#endif
