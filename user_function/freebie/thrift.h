#ifndef THRIFT_H
#define THRIFT_H

#include "gtypes.h"
#include "garray.h"

/* this macro is called to satisfy -Wall hardcore compilation */
#ifndef THRIFT_UNUSED_VAR
# define THRIFT_UNUSED_VAR(x) ((void) x)
#endif

static inline void
thrift_string_free (gpointer str)
{
	GByteArray *ptr = (GByteArray *) str;
	g_byte_array_unref (ptr);
}

#endif /* THRIFT_H */
