#include "parquet.h"
#include "thrift_file_transport.h"
#include "thrift_compact_protocol.h"

ThriftProtocol *
create_thrift_file_compact_protocol (gchar *filename, int file_size, int max_file_size, GError **error)
{
  ThriftTransport *transport;
  ThriftProtocol *protocol;
  GByteArray *r_buffer;

  /*
   * Create ThriftFileTransport
   */
  transport = g_object_new(THRIFT_TYPE_FILE_TRANSPORT, NULL);

  THRIFT_FILE_TRANSPORT(transport)->filename = filename;
  THRIFT_FILE_TRANSPORT(transport)->file_size = file_size;

  // NVMEV_FREEBIE_DEBUG("SLM Buffer: %lu, File Size: %d\n",
  //                       (size_t)THRIFT_FILE_TRANSPORT(transport)->filename, file_size);
  /*
   * load the total file into the r_buffer 
   */
  g_byte_array_unref(THRIFT_FILE_TRANSPORT(transport)->r_buffer);

  r_buffer = g_byte_array_new();
  r_buffer->len = file_size;
  r_buffer->data = filename;

  THRIFT_FILE_TRANSPORT(transport)->r_buffer = r_buffer; 
  THRIFT_FILE_TRANSPORT(transport)->r_buffer_location = 0; 
  THRIFT_FILE_TRANSPORT(transport)->location = 0; 
  THRIFT_FILE_TRANSPORT(transport)->max_file_size = max_file_size; 

  /*
   * Create ThriftCompactProtocol
   */
  protocol = g_object_new(THRIFT_TYPE_COMPACT_PROTOCOL, NULL);
  protocol->transport = transport;

  return protocol;
}

Value *
value_deepcpy (Value *dst, Value *src)
{
  if (IS_NULL_VALUE (src))
    return NULL_VALUE;

  dst->type = src->type;
  dst->len = src->len;

  if (IS_VALUE_TYPE_BOOLEAN (src))
  {
    dst->data.literal = src->data.literal;
  }
  else
  {
    g_assert (src->data.ptr != NULL, "Value type mismatch");
    dst->data.ptr = g_new (guint8, src->len);
    memcpy (dst->data.ptr, src->data.ptr, src->len);
  }

  return dst;
}
