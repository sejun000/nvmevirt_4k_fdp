#ifndef UTILS_H
#define UTILS_H

#include "gtypes.h"
#include "gtypes.h"
#include "gmem.h"

#define USING_LE_ARCH

// Little endian -------------------------------------------------------------
static inline gint32
le_to_i32 (guint8 *le)
{
#ifdef USING_LE_ARCH
  return *((gint32 *)le);
#else
  return ((*le & 0xFF) |
         ((*(le + 1) & 0xFF) << 8) |
         ((*(le + 2) & 0xFF) << 16) |
         ((*(le + 3) & 0xFF) << 24));
#endif
}

static inline gint64
le_to_i64 (guint8 *le)
{
#ifdef USING_LE_ARCH
  return *((gint64 *)le);
#else
  return (gint64) (le_to_i32 (le) & 0xFFFFFFFF) |
         ((gint64) (le_to_i32 (le + 4) & 0xFFFFFFFF) << 32);
#endif
}

static inline void
i32_to_le (gint32 i32, guint8 *le)
{
#ifdef USING_LE_ARCH
  // custom_memcpy (le, &i32, 4);
  memcpy (le, &i32, 4);
#else
  le[0] = i32 & 0xFF;
  le[1] = (i32 >> 8) & 0xFF;
  le[2] = (i32 >> 16) & 0xFF;
  le[3] = (i32 >> 24) & 0xFF;
#endif
}

static inline void
i64_to_le (gint64 i64, guint8 *le)
{
#ifdef USING_LE_ARCH
  // custom_memcpy (le, &i64, 8);
  memcpy (le, &i64, 8);
#else
  i32_to_le (i64, le);
  i32_to_le (i64 >> 32, le + 4);
#endif
}

// Varint --------------------------------------------------------------------
static inline gint32
int64_to_varint (gint64 i64, guint8 *varint)
{
  for (gint32 i = 0; i < 10; i++)
  {
    if ((i64 & ~0x7FL) == 0) {
      varint[i] = (gint8) i64;
      return i + 1;
    } else {
      varint[i] = (gint8) ((i64 & 0x7F) | 0x80);
      i64 >>= 7;
    }
  }

  g_assert (FALSE, "varint size too big");
  return -1;
}

static inline gint32
varint_to_int64 (guint8 *varint, gint64 *i64)
{
  *i64 = 0;
  for (gint32 i = 0; i < 10; i++)
  {
    *i64 |= (varint[i] & 0x7F) << (i * 7);
    if ((varint[i] & ~0x7FL) == 0)
      return i + 1;
  }

  g_assert (FALSE, "varint size too big");
  return -1;
}

static inline gint32
int32_to_varint (gint32 i32, guint8 *varint)
{
  for (gint32 i = 0; i < 5; i++)
  {
    if ((i32 & ~0x7FL) == 0) {
      varint[i] = (gint8) i32;
      return i + 1;
    } else {
      varint[i] = (gint8) ((i32 & 0x7F) | 0x80);
      i32 >>= 7;
    }
  }

  g_assert (FALSE, "varint size too big");
  return -1;
}

static inline gint32
varint_to_int32 (guint8 *varint, gint32 *i32)
{
  *i32 = 0;
  for (gint32 i = 0; i < 10; i++)
  {
    *i32 |= (varint[i] & 0x7F) << (i * 7);
    if ((varint[i] & ~0x7FL) == 0)
      return i + 1;
  }

  g_assert (FALSE, "varint size too big");
  return -1;
}

/*
 * @see https://github.com/duckdb/duckdb/blob/v1.2.0/extension/parquet/include/decode_utils.hpp#L139-L146
 */
static inline gint8
get_varint_size (guint32 val)
{
  guint8 res = 0;
  do {
    val >>= 7;
    res++;
  } while (val != 0);
  return res;
}

// Etc. ----------------------------------------------------------------------
static inline guint64
string_copy (gchar **out, const gchar *in)
{
  guint64 len;
  gchar *buf;

  len = strlen (in);
  buf = g_new0 (gchar, len + 1);

  strcpy (buf, in);
  // custom_memcpy (buf, in, len);

  *out = buf;

  return len;
}

static inline gint64
power (gint64 x, gint64 y)
{
#ifndef KERNEL_MODE
  return (gint64) pow (x, y);
#else
  gint64 ret = 1;
  for (gint i = 0; i < y; i++)
    ret *= x;
  return ret;
#endif
}

#define LOG(string, ...) printk (KERN_INFO string, __VA_ARGS__)

#endif /* UTILS_H */
