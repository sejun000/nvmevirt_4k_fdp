#ifndef G_OBJECT_H
#define G_OBJECT_H

#include "gtypes.h"
#include "gobject-type.h"

typedef struct _GObject GObject;
struct _GObject {
  GTypeInstance g_type_instance;

  // XXX: not yet atomic
  guint ref_count;
};

gpointer g_object_new (GType object_type, const gchar *first_property_name,
                       ...);

void g_object_unref (gpointer object);

gpointer g_object_ref (gpointer object);

void g_object_init (void);
void check_g_object (void);

#define G_OBJECT(object) \
  (G_TYPE_CHECK_INSTANCE_CAST ((object), G_TYPE_OBJECT, GObject))

/*
 * Let:
 *   C  := # of columns (max. 16)
 *   RG := # of row groups (max. 1)
 *   F  := # of in/out files (max. 32 input file + 16 output file = 48)
 *
 * Then the max. number of ColumnChunk/ColumnMetaData/Statistics is C * RG * F
 * (768).
 */
#define STATISTICS_POOL_SIZE (768 * 16)
#define COLUMN_CHUNK_POOL_SIZE (768 * 16)
#define COLUMN_META_DATA_POOL_SIZE (768 * 16)

#endif /* G_OBJECT_H */
