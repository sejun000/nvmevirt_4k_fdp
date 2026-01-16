#ifndef G_MEM_H
#define G_MEM_H

#include "gtypes.h"

#define USE_GLIBC

extern void *__memmove_avx_unaligned_erms(void *dst, const void *src, size_t size);

static inline void custom_memcpy(void *dst, const void *src, size_t len)
{
#ifdef USE_GLIBC 
    __memmove_avx_unaligned_erms(dst, src, len);
#else
    /*
     * "cld" ensures the direction flag is cleared (move forward).
     * "=D", "=S", "=c" mark updated outputs for RDI, RSI, RCX.
     * "0", "1", "2" tie those outputs to the matching inputs.
     * "memory" tells the compiler this asm changes memory (so it won’t reorder).
     */
    __asm__ volatile (
        "cld\n\t"          /* Ensure direction flag is cleared */
        "rep movsb"        /* Copy RCX bytes from [RSI] to [RDI] */
        : "=D" (dst), "=S" (src), "=c" (len)
        : "0" (dst),  "1" (src),  "2" (len)
        : "memory"
    );
#endif
}

static inline void
g_free (gpointer mem)
{
#ifndef KERNEL_MODE
  free (mem);
#else
  kvfree (mem);
#endif
}

static inline void
g_free_sized (gpointer mem, gsize size)
{
  /* satisfy -Wall */
  ((void) size);
#ifndef KERNEL_MODE
  free (mem);
#else
  kvfree (mem);
#endif
}

static inline gpointer
g_malloc (gsize n_bytes)
{
#ifndef KERNEL_MODE
  return malloc (n_bytes);
#else
	return kvmalloc_node (n_bytes, GFP_KERNEL, 1);
	// gpointer *ret;
	// ktime_t start, end;
	// s64 duration_ns;

	// start = ktime_get();
	// ret = kvmalloc (n_bytes, GFP_KERNEL);
	// end = ktime_get();
	// duration_ns = ktime_to_ns(ktime_sub(end, start));
	// printk(KERN_INFO "malloc : %lld ns\n", duration_ns);
	// return ret;
#endif
}

static inline gpointer
g_malloc0 (gsize n_bytes)
{
#ifndef KERNEL_MODE
  return calloc (1, n_bytes);
#else
	return kvzalloc_node (n_bytes, GFP_KERNEL, 1);
#endif
}

static inline gpointer
g_realloc (gpointer mem, gsize n_bytes)
{
#ifndef KERNEL_MODE
  return realloc (mem, n_bytes);
#else
  gpointer new = g_malloc (n_bytes);
  if (!new)
    return NULL;
  if (mem)
  {
    custom_memcpy (new, mem, n_bytes);
    // memcpy (new,mem, n_bytes);
    g_free (mem);
  }
  return new;
#endif
}

#define g_new(struct_type, n_structs) \
  ((struct_type *) g_malloc ((n_structs) * sizeof (struct_type)))

#define g_new0(struct_type, n_structs) \
  ((struct_type *) g_malloc0 ((n_structs) * sizeof (struct_type)))

static inline gpointer
g_steal_pointer (gpointer pp)
{
  gpointer *ptr = (gpointer *) pp;
  gpointer ref;

  ref = *ptr;
  *ptr = NULL;

  return ref;
}

#endif /* G_MEM_H */
