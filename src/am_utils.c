#include <string.h>
#include "defn.h"

// Compares variables depending on their type (input v_type)
// Output similar to strcmp (only with -2 output for input errors)
//THELEI TESTING OPWSDIPOTE
int v_cmp(char v_type, void* value1, void* value2) {
  if (v_type == INTEGER) {
    if ((*(int*) value1) < (*(int*) value2))
      return -1;
    else if ((*(int*) value1) > (*(int*) value2))
      return 1;
    else
      return 0;
  }
  else if (v_type == FLOAT) {
    if ((*(float *)value1) < (*(float *)value2))
      return -1;
    else if ((*(float *)value1) > (*(float *)value2))
      return 1;
    else
      return 0;
  }
  else if (v_type == STRING)
    return strcmp((char *)value1, (char *)value2);
  // In case of wrong v_type input
  else
    return -2;
}

int init_bptree(int fileDesc, void *value1, void *value2) {
  // Initialize block and get file descriptor
  BF_Block *block;
  BF_Block_Init(&block);
  int fd = OpenIndexes[fileDesc].fd;
  // Get block data
  CHK_BF_ERR(BF_GetBlock(fd, block_id, block));
  char* block_data = BF_Block_GetData(block);

  // Initialize data block with id, record number (1), next data block id (-1),
  // and finally the record (value1, value2)
  char* block_data = BF_Block_GetData(block);
  // It is a "data block"
  char did[4] = ".db";
  memcpy(block_data, did, 4);
  block_data += 4;
  // One record will be inserted
  int temp_i = 1;
  memcpy(block_data, &temp_i, sizeof(int));
  block_data += sizeof(int);
  // There is no next data block (id = -1)
  temp_i = -1;
  memcpy(block_data, &temp_i, sizeof(int));
  block_data += sizeof(int);
  // Copy input values
  memcpy(block_data, &value1, OpenIndexes[fileDesc].attrLength1);
  block_data += OpenIndexes[fileDesc].attrLength1;
  memcpy(block_data, &value2, OpenIndexes[fileDesc].attrLength2);

  // Get block id of the data block and save it in OpenIndexes
  int block_id = 0;
  CHK_BF_ERR(BF_GetBlockCounter(fileDesc, &block_id));
  block_id--;
  OpenIndexes[fileDesc].dataBlockNum = block_id;

  // Set dirty and unpin block
  BF_Block_SetDirty(block);
  CHK_BF_ERR(BF_UnpinBlock(block));

  // Allocate block for root
  CHK_BF_ERR(BF_AllocateBlock(fd, block));
  // Initialize root block with id and key_number (1)
  block_data = BF_Block_GetData(block);
  // It is an "index block"
  char rid[4] = ".ib";
  memcpy(block_data, rid, 4);
  block_data += 4;
  // One key will be inserted
  temp_i = 1;
  memcpy(block_data, &temp_i, sizeof(int));
  block_data += sizeof(int);
  // Block number before the first key does not exist yet (-1)
  temp_i = -1;
  memcpy(block_data, &temp_i, sizeof(int));
  block_data += sizeof(int);
  // Finally save key, then data block number in the root
  memcpy(block_data, &value1, OpenIndexes[fileDesc].attrLength1);
  block_data += OpenIndexes[fileDesc].attrLength1;
  memcpy(block_data, &OpenIndexes[fileDesc].dataBlockNum, sizeof(int));

  // Get block id of the root and save it in OpenIndexes
  CHK_BF_ERR(BF_GetBlockCounter(fd, &block_id));
  block_id--;
  OpenIndexes[fileDesc].rootBlockNum = block_id;

  // Set dirty, unpin and destroy block
  BF_Block_SetDirty(block);
  CHK_BF_ERR(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  return AME_OK;
}

int create_leftmost_block(int fileDesc, void *value1, void *value2) {
  // Initialize block and get file descriptor
  BF_Block *block;
  BF_Block_Init(&block);
  int fd = OpenIndexes[fileDesc].fd;
  // Allocate leftmost data block
  CHK_BF_ERR(BF_AllocateBlock(fd, block));

  // Initialize data block with id, record number (1), next data block,
  // (previous first block) and finally the record (value1, value2)
  char* block_data = BF_Block_GetData(block);
  // It is a "data block"
  char did[4] = ".db";
  memcpy(block_data, did, 4);
  block_data += 4;
  // One record will be inserted
  int temp_i = 1;
  memcpy(block_data, &temp_i, sizeof(int));
  block_data += sizeof(int);
  // Next data block is the previous first block
  temp_i = OpenIndexes[fileDesc].dataBlockNum;
  memcpy(block_data, &temp_i, sizeof(int));
  block_data += sizeof(int);
  // Copy input values
  memcpy(block_data, &value1, OpenIndexes[fileDesc].attrLength1);
  block_data += OpenIndexes[fileDesc].attrLength1;
  memcpy(block_data, &value2, OpenIndexes[fileDesc].attrLength2);

  // Get block id of the data block and save it in OpenIndexes
  int block_id = 0;
  CHK_BF_ERR(BF_GetBlockCounter(fileDesc, &block_id));
  block_id--;
  OpenIndexes[fileDesc].dataBlockNum = block_id;

  // Set dirty and unpin and destroy block
  BF_Block_SetDirty(block);
  CHK_BF_ERR(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  return AME_OK;
}

RecTravOut rec_trav_insert(int fileDesc, int block_id, void *value1, void *value2) {
  // Initialize block and get file descriptor
  BF_Block *block;
  BF_Block_Init(&block);
  int fd = OpenIndexes[fileDesc].fd;
  // Get block data
  CHK_BF_ERR(BF_GetBlock(fd, block_id, block));
  char* block_data_strt = BF_Block_GetData(block);
  // Allocate space for a key value
  void* temp_key = (void *)malloc(OpenIndexes[fileDesc].attrLength1);
  // First check if current block is an index block or a data block
  // If it is an index block, continue traversal (recursion)
  // (in order to find the right data block for the values to be inserted into)
  if (strcmp(block_data_strt, ".ib") == 0) {
    // Get number of keys in block
    char* block_data = block_data_strt + 4;
    int key_num = 0;
    memcpy(&key_num, block_data, sizeof(int));
    // Get first key
    block_data += 2*sizeof(int);
    memcpy(temp_key, block_data, OpenIndexes[fileDesc].attrLength1);

    // Then try to find next block
    while(key_num < 1 &&
          v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) > -1) {
      // Move on to the next key
      block_data += OpenIndexes[fileDesc].attrLength1 + sizeof(int);
      key_num--;
      memcpy(temp_key, block_data, OpenIndexes[fileDesc].attrLength1);
    }
    // Only if it is the last key go to right pointer
    if (v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) > -1)
      block_data += OpenIndexes[fileDesc].attrLength1;
    // Else normally go to left pointer
    else
      block_data -= sizeof(int);

    int found_block_id = 0;
    memcpy(&found_block_id, block_data, sizeof(int));

    // Before the recursive call
    block_data_strt = NULL;
    block_data = NULL;
    CHK_BF_ERR(BF_UnpinBlock(root_block));
    // Then call the same function with found_block_num as input block number
    RecTravOut possible_block = rec_trav_insert(fileDesc, found_block_id, value1, value2);

    // If the returned struct's possible_block.nblock_id is not -1, that
    // means that a new block has been created in a lower level, so a key and a
    // pointer to it should be added in this block
    if (possible_block.nblock_id != -1) {
      CHK_BF_ERR(BF_GetBlock(fd, block_id, block));
      block_data_strt = BF_Block_GetData(block);
      block_data = block_data_strt + 4;

      // Get number of keys
      int key_num = 0;
      memcpy(&key_num, block_data, sizeof(int));
      // Calculate used space and check if a new key fits in the block
      int key_plus_ptr_size = sizeof(int) + OpenIndexes[fileDesc].attrLength1;
      int used_space = 4 + 2*sizeof(int) + key_num*key_plus_ptr_size;
      // If it fits, then insert it
      if (used_space + key_plus_ptr_size <= BF_BLOCK_SIZE) {

      }
      // Else, create a new index block and distribute the keys between them
      else {

      }
    // Else just return the same output (possible_block with nblock_id == -1)
    }
    else
      return possible_block;
  }
  // Else, if it is a data block, that means that this is where the given
  // values should be inserted (end recursion)
  else if (strcmp(block_data_strt, ".db") == 0) {
    // Get number of records (consist of 2 attributes) in block
    char* block_data = block_data_strt + 4;
    int record_num = 0;
    memcpy(&record_num, block_data, sizeof(int));

    // Check if the new record fits in the block
    int record_size = OpenIndexes[fileDesc].attrLength1 +
                      OpenIndexes[fileDesc].attrLength2;
    int used_space = 4 + 2*sizeof(int) + record_num*record_size;
    // If it fits, then insert it
    if (used_space + record_size <= BF_BLOCK_SIZE) {
      // Get first record key value
      block_data += 2*sizeof(int);
      memcpy(temp_key, block_data, OpenIndexes[fileDesc].attrLength1);
      // Find out where its position should be
      int move_pos = 0;
      while (move_pos < record_num &&
            v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) >= 0) {
        move_pos++;
        block_data += record_size;
        // Move on to the next key
        memcpy(temp_key, block_data, OpenIndexes[fileDesc].attrLength1);
      }
      // DEN KSERW AN THA THELEI IF EDW GIA ISO TOU REC_NUM
      int copy_rec_num = record_num - move_pos;
      char* buffer = malloc(copy_rec_num * record_size);
      memcpy(buffer, block_data, copy_rec_num * record_size);
      // Insert new record
      memcpy(block_data, value1, OpenIndexes[fileDesc].attrLength1);
      block_data += OpenIndexes[fileDesc].attrLength1;
      memcpy(block_data, value2, OpenIndexes[fileDesc].attrLength2);
      block_data += OpenIndexes[fileDesc].attrLength2;
      // Move all records that come after that 1 position to the right
      memcpy(block_data, buffer, copy_rec_num * record_size);

      // Free buffer
      free(buffer);

      // Increment the block's second value (number of records in it) by one
      block_data = block_data_strt + 4;
      int new_record_num = 0;
      memcpy(&new_record_num, block_data, sizeof(int));
      new_record_num++;
      memcpy(block_data, &new_record_num, sizeof(int));

      // Set dirty and unpin block
      BF_Block_SetDirty(block);
      CHK_BF_ERR(BF_UnpinBlock(block));
    }
    // Else, create a new data block and distribute the records between them in
    // the best way possible (records with the same key value should not be
    // separated)
    else {
      // First copy all records to a buffer that has enough space for one more
      char* buffer_strt = malloc((record_num + 1) * record_size);
      char* buffer
      block_data += 2*sizeof(int);
      memcpy(buffer, block_data, (record_num + 1) * record_size);
      // Find out where the new record's position should be in the buffer
      memcpy(temp_key, buffer, OpenIndexes[fileDesc].attrLength1);
      int move_pos = 0;
      while (move_pos < record_num &&
            v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) >= 0) {
        move_pos++;
        block_data += record_size;
        memcpy(temp_key, block_data, OpenIndexes[fileDesc].attrLength1);
      }
      // DEN KSERW AN THA THELEI IF EDW GIA ISO TOU REC_NUM
      int copy_rec_num = record_num - move_pos;
      char* buffer = malloc(copy_rec_num * record_size);
      memcpy(buffer, block_data, copy_rec_num * record_size);
      // Insert new record
      memcpy(block_data, value1, OpenIndexes[fileDesc].attrLength1);
      block_data += OpenIndexes[fileDesc].attrLength1;
      memcpy(block_data, value2, OpenIndexes[fileDesc].attrLength2);
      block_data += OpenIndexes[fileDesc].attrLength2;
      // Move all records that come after that 1 position to the right
      memcpy(block_data, buffer, copy_rec_num * record_size);

      // Free buffer
      free(buffer_strt);
    }
  }

  free(temp_key);
  BF_Block_Destroy(&block);

}
