#include "gerror.h"

void
g_set_error (GError **err, GQuark domain, gint code, const gchar *format, ...)
{
  GError *new;
  va_list args;

  if (err == NULL)
    return;

  va_start (args, format);
  new = g_new (GError, 1);
  new->domain = domain;
  new->code = code;
  new->message = g_new (gchar, 32);
  sprintf (new->message, format, args);
  va_end (args);
  printk("%s\n", new->message);
  g_assert(false, "Error");

  if (*err == NULL)
    *err = new;
}
