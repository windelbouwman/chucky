
// Stubs:
void copy_blob(int offset, int size);

//
struct {
  int offset;
  int size;
  int hash;
} chunk_t;

struct {
  int n_chunks;
  struct chunk_t* p_chunks;
} meta_data_t;


/*
 * Update routine.
 *
 * Loop over all chunks and check if we already have the blob in the old
 * firmware image.
 *
 * If the blob is in the other image, load it from there. Otherwise, download
 * the blob.
 */
void update_routine()
{
  int i;
  for (i = 0; i < meta_data->n_chunks; i++)
  {
    int hash = 0;
    if (has_blob_locally(hash))
    {
      copy_blob();
    }
    else
    {
      download_blob();
    }
  }
}

void has_blob_locally()
{
  // TODO
  return false;
}

