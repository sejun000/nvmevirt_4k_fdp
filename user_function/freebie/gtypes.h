#ifndef G_TYPES_H
#define G_TYPES_H

#include "c_standard_libs.h"

// glib/glibconfig.h ---------------------------------------------------------

typedef signed char gint8;
typedef unsigned char guint8;
typedef signed short gint16;
typedef unsigned short guint16;
typedef signed int gint32;
typedef unsigned int guint32;
typedef signed long gint64;
typedef unsigned long guint64;
typedef unsigned long gsize;
typedef long gssize;

#ifndef KERNEL_MODE
typedef gint64 goffset;
#else
typedef loff_t goffset;
#endif

#define G_MAXINT32	((gint32)  0x7fffffff)

#define G_GINT64_CONSTANT(val)  (val##L)

#define GINT_TO_POINTER(i)  ((gpointer) (glong) (i))
#define GPOINTER_TO_INT(p)  ((gint)  (glong) (p))

#define G_MAXUINT UINT_MAX

// glib/gtypes.h -------------------------------------------------------------

#define FALSE 0
#define TRUE 1

typedef char   gchar;
typedef long   glong;
typedef int    gint;
typedef gint   gboolean;

typedef unsigned char   guchar;
typedef unsigned long   gulong;
typedef unsigned int    guint;

typedef float   gfloat;
typedef double  gdouble;

typedef void* gpointer;
typedef const void *gconstpointer;
typedef void (*GDestroyNotify)(void*);

// glib/gmacros.h ------------------------------------------------------------

#define MAX(a, b) (a > b ? a : b)
#define MIN(a, b) (a < b ? a : b)
#define G_LIKELY(x) (__builtin_expect((x),1))
#define G_UNLIKELY(x) (__builtin_expect((x),0))

// glib/gutilsprivate.h ------------------------------------------------------

static inline gsize
g_nearest_pow (gsize num)
{
  gsize n = num - 1;

  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;

  return n + 1;
}

// glib/gtestutils.h ---------------------------------------------------------

#ifndef KERNEL_MODE
#define g_assert(expr, msg) assert((expr) && (msg))
#else
#define g_assert(expr, msg) do { \
    if (!(expr)) { \
        printk(KERN_ERR "%s\n", msg); \
        BUG(); \
    } \
  } while (0)
#endif
#endif /* G_TYPES_H */
