#ifndef G_ERROR_H
#define G_ERROR_H

#include "gtypes.h"
#include "gmem.h"

// GQuark (glib/gquark.h) ----------------------------------------------------
enum _GQuark {
  G_QUARK_THRIFT_PROTOCOL_ERROR = 0,
  G_QUARK_THRIFT_TRANSPORT_ERROR,
};
typedef enum _GQuark GQuark;

// GError --------------------------------------------------------------------
typedef struct _GError GError;
struct _GError
{
  GQuark       domain;
  gint         code;
  gchar       *message;
};

void
g_set_error (GError **err, GQuark domain, gint code, const gchar *format, ...);

static inline void
g_error_free (GError *error)
{
  if (!error)
    return;

  if (!error->message)
    g_free (error->message);

  g_free (error);
}

#endif /* G_ERROR_H */
