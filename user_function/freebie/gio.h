#ifndef G_IO_H
#define G_IO_H

#include "c_standard_libs.h"
#include "gtypes.h"

#ifndef KERNEL_MODE
typedef gint FD;
#else
typedef struct file * FD;
#endif

static inline FD
g_io_open (const gchar *pathname, gint flags)
{
#ifndef KERNEL_MODE
  FD fd;
  fd = open (pathname, flags, 0644);
  return fd < 0 ? (0 - errno) : fd;
#else
  return filp_open (pathname, flags, 0644);
#endif
}

static inline gint
g_io_close (FD fd)
{
#ifndef KERNEL_MODE
  gint ret;
  ret = close (fd);
  return ret;
#else
  if (!fd)
    return -1;
  return filp_close (fd, NULL);
#endif
}

static inline gssize
g_io_pread (FD fd, gpointer buf, gsize count, goffset offset)
{
#ifndef KERNEL_MODE
  gssize ret;
  ret = pread (fd, buf, count, offset);
  return ret;
#else
  if (!fd)
    return -1;
  return kernel_read (fd, buf, count, &offset);
#endif
}

static inline gssize
g_io_pwrite (FD fd, gpointer buf, gsize count, goffset offset)
{
#ifndef KERNEL_MODE
  gssize ret;
  ret = pwrite (fd, buf, count, offset);
  return ret;
#else
  if (!fd)
    return -1;
  return kernel_write (fd, buf, count, &offset);
#endif
}

static inline gint
g_io_fsync (FD fd)
{
#ifndef KERNEL_MODE
  gint ret;
  ret = fsync (fd);
  return ret;
#else
  vfs_fsync (fd, 0);
  return 0;
#endif
}

static inline goffset
g_io_seek (FD fd, goffset offset, gint whence)
{
#ifndef KERNEL_MODE
  goffset ret;
  ret = lseek (fd, offset, whence);
  return ret;
#else
  if (!fd)
    return -1;
  return vfs_llseek (fd, offset, whence);
#endif
}

#endif /* G_IO_H */
