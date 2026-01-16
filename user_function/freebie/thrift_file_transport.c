#include "c_standard_libs.h"

#include "garray.h"
#include "gerror.h"
#include "gio.h"
#include "gtypes.h"
#include "thrift_transport.h"
#include "thrift_file_transport.h"

#ifdef KERNEL_MODE
#include <linux/time.h>
#else
#include <time.h>
#endif

// #define READ_TEST
// #define WRITE_TEST

#ifndef KERNEL_MODE
struct timespec tt1, tt2;
unsigned long long start, end;
#else
ktime_t start, end;
s64 duration_ns;
#endif


unsigned long long total;

GType
thrift_file_transport_get_type (void)
{
  return G_TYPE_THRIFT_FILE_TRANSPORT;
}

/* implements thrift_transport_open */
gboolean
thrift_file_transport_open (ThriftTransport *transport, GError **error)
{
  ThriftFileTransport *t;
  FD fd;

//   t = THRIFT_FILE_TRANSPORT (transport);

//   g_assert (t->filename != NULL, "file name not set");

// #ifndef KERNEL_MODE
//   if (t->fd > 0)
// #else
//   if (t->fd != NULL)
// #endif
//     return TRUE;

//   fd = g_io_open (t->filename, O_RDWR | O_CREAT);

// #ifndef KERNEL_MODE
//   if (fd < 0)
// #else
//   if (IS_ERR(fd))
// #endif
//   {
//     g_set_error (error,
//                  THRIFT_TRANSPORT_ERROR,
//                  THRIFT_TRANSPORT_ERROR_CLOSE,
//                  "File open error");

//     return FALSE;
//   }

//   t->fd = fd;

  return TRUE;
}

/* implements thrift_transport_close */
gboolean
thrift_file_transport_close (ThriftTransport *transport, GError **error)
{
  ThriftFileTransport *t;

  t = THRIFT_FILE_TRANSPORT (transport);

  // if (g_io_close (t->fd) != 0)
  // {
  //   g_set_error (error,
  //                THRIFT_TRANSPORT_ERROR,
  //                THRIFT_TRANSPORT_ERROR_CLOSE,
  //                "File close error");

  //   return FALSE;
  // }

  return TRUE;
}

gint32
thrift_file_transport_read (ThriftTransport *transport, gpointer buf,
                            guint32 len, GError **error)
{
  ThriftFileTransport *t;
  goffset location;

  t = THRIFT_FILE_TRANSPORT (transport);

	if (t->r_buffer != NULL &&
			t->location + len <= t->r_buffer_location + t->r_buffer->len)
	{
		location = t->location - t->r_buffer_location;
#ifdef READ_TEST
#ifdef KERNEL_MODE
		start = ktime_get();
#else
		clock_gettime(CLOCK_REALTIME, &tt1);
#endif
#endif
		custom_memcpy (buf, t->r_buffer->data + location, len);
	  // memcpy (buf, t->r_buffer->data + location, len);
#ifdef READ_TEST
#ifdef KERNEL_MODE
		end = ktime_get();

		duration_ns = ktime_to_ns(ktime_sub(end, start));
		total += duration_ns;
#else
		clock_gettime(CLOCK_REALTIME, &tt2);

		start = (tt1.tv_sec * 1000000000L)+tt1.tv_nsec;
		end = (tt2.tv_sec * 1000000000L)+tt2.tv_nsec;
		total += (double)(end-start);
		printf("FILE I/O time  : %14.4lf us\n", (double)total/1000);
#endif
#endif
	}

  else
  {
    // Prevent -Wall
    ((void) error);
    g_assert (FALSE, "read not prefetched");
  }

  t->location += len;
  return len;
}

/* implements thrift_transport_write */
gboolean
thrift_file_transport_write (ThriftTransport *transport,
                             const gpointer buf, const guint32 len,
                             GError **error)
{
  ThriftFileTransport *t;
  guint8 *_buf;
  guint32 _len;
  gssize n;

	t = THRIFT_FILE_TRANSPORT (transport);
	_buf = (guint8 *) buf;
	_len = len;

#ifdef WRITE_TEST
#ifdef KERNEL_MODE
		start = ktime_get();     // End time
#else
		clock_gettime(CLOCK_REALTIME, &tt1);
#endif
#endif
    if (t->location + _len > t->max_file_size) {
      printk("Writing behind the allocated size (allocated %llu, current wrote: %llu, writing %u)", 
                    t->max_file_size, t->location, _len);
      BUG();
      // g_assert(false, "Writing behind allocated region");
    }
    custom_memcpy(t->r_buffer->data + t->location, _buf, _len);
    // memcpy (t->r_buffer->data + t->location, _buf, _len);
    t->location += _len;
#ifdef WRITE_TEST
#ifdef KERNEL_MODE
		end = ktime_get();     // End time

		duration_ns = ktime_to_ns(ktime_sub(end, start));
		total += duration_ns;
		printk(KERN_INFO "WRITE took %lld ns\n", total);
#else
		clock_gettime(CLOCK_REALTIME, &tt2);

		start = (tt1.tv_sec * 1000000000L)+tt1.tv_nsec;
		end = (tt2.tv_sec * 1000000000L)+tt2.tv_nsec;
		total += (double)(end-start);
		printf("File I/O time  : %14.4lf us\n", (double)total/1000);
#endif
#endif

  return TRUE;
}

/* implements thrift_transport_flush */
gboolean
thrift_file_transport_flush (ThriftTransport *transport, GError **error)
{
  // ThriftFileTransport *t;

  // t = THRIFT_FILE_TRANSPORT (transport);
  // if (g_io_fsync (t->fd) == -1)
  // {
  //   g_set_error (error,
  //                THRIFT_TRANSPORT_ERROR,
  //                THRIFT_TRANSPORT_ERROR_UNKNOWN,
  //                "Failed to flush");
  //   return FALSE;
  // }

  return TRUE;
}

/* initializes the instance */
void
thrift_file_transport_init (ThriftFileTransport *transport)
{
#ifndef KERNEL_MODE
  transport->fd = -1;
#else
  // transport->fd = NULL;
#endif
  transport->filename = NULL;

  transport->location = 0;

  transport->max_file_size = 0;
  transport->r_buffer = NULL;
  transport->r_buffer_location = -1;
}

/* destructor */
void
thrift_file_transport_finalize (GObject *object)
{
  ThriftFileTransport *file_transport;

  file_transport = THRIFT_FILE_TRANSPORT(object);

  g_byte_array_unref_no_data(file_transport->r_buffer);
  file_transport->r_buffer = NULL;
}

gboolean
thrift_file_transport_prefetch (ThriftTransport *transport, goffset pos,
                                gsize len, GError **error)
{
  ThriftFileTransport *t;
  gssize n;
  gsize want_len;
  goffset want_pos;
  goffset append_idx;
  goffset r_buffer_end;
  goffset want_end;

  t = THRIFT_FILE_TRANSPORT (transport);

	want_len = len;
	want_pos = pos;
	append_idx = 0;

#ifdef READ_TEST
#ifdef KERNEL_MODE
	start = ktime_get();     // End time
#else
	clock_gettime(CLOCK_REALTIME, &tt1);
#endif
#endif

  /*
   * Determine whether the r_buffer should be created or expanded.
   */
  if (t->r_buffer != NULL)
  {
    r_buffer_end = t->r_buffer_location + t->r_buffer->len;

    if (t->r_buffer_location <= pos && pos < r_buffer_end)
    {
      want_end = pos + len;

			/*
			 * Requested range already buffered
			 */
			if (t->r_buffer_location <= want_end && want_end <= r_buffer_end)
			{
#ifdef READ_TEST
#ifdef KERNEL_MODE
				end = ktime_get();     // End time

				duration_ns = ktime_to_ns(ktime_sub(end, start));
				total += duration_ns;
				printk(KERN_INFO "READ took %lld ns\n", total);
#else
				clock_gettime(CLOCK_REALTIME, &tt2);

				start = (tt1.tv_sec * 1000000000L)+tt1.tv_nsec;
				end = (tt2.tv_sec * 1000000000L)+tt2.tv_nsec;
				total += (double)(end-start);
				printf("File I/O time  : %14.4lf us\n", (double)total/1000);
#endif
#endif
				return TRUE;
			}

      /*
       * Expand the r_buffer
       */
      else
      {
        printk("want_end: %llu, r_buffer_end: %llu\n", want_end, r_buffer_end);
        g_assert(false, "File should be always loaded in memory 1");
        want_len = want_end - r_buffer_end;
        want_pos = r_buffer_end;
        append_idx = t->r_buffer->len;
        g_byte_array_maybe_expand (t->r_buffer, t->r_buffer->len + want_len);
        t->r_buffer->len += want_len;
      }
    }

    /*
     * Overwrite existing r_buffer
     */
    else
    {
      g_assert(false, "File should be always loaded in memory 2");
      g_byte_array_maybe_expand (t->r_buffer, want_len);
      t->r_buffer->len = want_len;
      t->location = want_pos;
      t->r_buffer_location = want_pos;
    }
  }

  /*
   * Create new r_buffer
   */
  else
  {
    g_assert(false, "File should be always loaded in memory 3");
    t->r_buffer = g_byte_array_sized_new (want_len);
    t->r_buffer->len = want_len;
    t->location = want_pos;
    t->r_buffer_location = want_pos;
  }

	/*
	 * Do I/O
	 */
	// n = g_io_pread (t->fd, t->r_buffer->data + append_idx,
	// 		want_len, want_pos);
	// if (n == -1)
	// {
	// 	g_set_error (error,
	// 			THRIFT_TRANSPORT_ERROR,
	// 			THRIFT_TRANSPORT_ERROR_RECEIVE,
	// 			"Failed to read from fd");
	// 	return FALSE;
	// }

#ifdef READ_TEST
#ifdef KERNEL_MODE
	end = ktime_get();     // End time

	duration_ns = ktime_to_ns(ktime_sub(end, start));
	total += duration_ns;
	printk(KERN_INFO "READ took %lld ns\n", total);
#else
	clock_gettime(CLOCK_REALTIME, &tt2);

	start = (tt1.tv_sec * 1000000000L)+tt1.tv_nsec;
	end = (tt2.tv_sec * 1000000000L)+tt2.tv_nsec;
	total += (double)(end-start);
	printf("File I/O time  : %14.4lf us\n", (double)total/1000);
#endif
#endif

  return TRUE;
}

void print_file_transport_time(void)
{
#ifdef READ_TEST
	printk(KERN_INFO "READ took %lld ns\n", total);
#endif
#ifdef WRITE_TEST 
	printk(KERN_INFO "WRITE took %lld ns\n", total);
#endif
}

goffset
thrift_file_transport_get_location (ThriftTransport *transport)
{
  return THRIFT_FILE_TRANSPORT (transport)->location;
}

void
thrift_file_transport_set_location (ThriftTransport *transport, goffset location)
{
  THRIFT_FILE_TRANSPORT (transport)->location = location;
}

gssize
thrift_file_transport_get_size (ThriftTransport *transport, GError **error)
{
  ThriftFileTransport *t;

  t = THRIFT_FILE_TRANSPORT (transport);
  return t->file_size;
}
