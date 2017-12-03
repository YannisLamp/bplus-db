#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "bf.h"
#include "AM.h"
#include "defn.h"
#include "globl_structs.h"
#include "am_utils.h"

// Error check for bf level calls
#define CHK_BF_ERR(call)             \
  {                                  \
    BF_ErrorCode code = call;        \
    if (code != BF_OK) {             \
      BF_PrintError(code);           \
			AM_errno = AME_BF_CALL_ERROR;  \
      return AME_BF_CALL_ERROR;      \
    }                                \
  }

#define CHK_BF_ERR_RECTRAV(call)                \
  {                                             \
    BF_ErrorCode code = call;                   \
    if (code != BF_OK) {                        \
      BF_PrintError(code);                      \
			AM_errno = AME_BF_CALL_ERROR;             \
      possible_block.error = AME_BF_CALL_ERROR; \
      return possible_block;                    \
    }                                           \
  }

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
  else if (v_type == STRING) {
    if (strcmp((char *)value1, (char *)value2) < 0)
      return -1;
    else if (strcmp((char *)value1, (char *)value2) > 0)
      return 1;
    else
      return 0;
  }
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
  CHK_BF_ERR(BF_AllocateBlock(fd, block));
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
  memcpy(block_data, value1, OpenIndexes[fileDesc].attrLength1);
  memcpy(block_data + OpenIndexes[fileDesc].attrLength1,
         value2, OpenIndexes[fileDesc].attrLength2);

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
  memcpy(block_data, value1, OpenIndexes[fileDesc].attrLength1);
  memcpy(block_data + OpenIndexes[fileDesc].attrLength1,
        &OpenIndexes[fileDesc].dataBlockNum, sizeof(int));

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
  memcpy(block_data, value1, OpenIndexes[fileDesc].attrLength1);
  memcpy(block_data + OpenIndexes[fileDesc].attrLength1,
         value2, OpenIndexes[fileDesc].attrLength2);

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
  //printf("REC CALLEED\n" );
  // Function output
  RecTravOut possible_block;
  // Initialize block and get file descriptor
  BF_Block *block;
  BF_Block_Init(&block);
  int fd = OpenIndexes[fileDesc].fd;
  // Get block data
  CHK_BF_ERR_RECTRAV(BF_GetBlock(fd, block_id, block));
  char* block_data = BF_Block_GetData(block);
  // Allocate space for a key value
  void* temp_key = malloc(OpenIndexes[fileDesc].attrLength1);

  // First check if current block is an index block or a data block
  // If it is an index block, continue traversal (recursion)
  // (in order to find the right data block for the values to be inserted into)
  if (strcmp(block_data, ".ib") == 0) {
    //printf("IN INDEX\n" );
    // Get number of keys in block
    block_data += 4;
    int key_num = 0;
    memcpy(&key_num, block_data, sizeof(int));
    block_data += sizeof(int);
    // Get first key
    memcpy(temp_key, block_data + sizeof(int), OpenIndexes[fileDesc].attrLength1);
    int key_plus_ptr_size = sizeof(int) + OpenIndexes[fileDesc].attrLength1;
    // Then try to find next block
    int found_block_pos = 0;
    //printf("%c\n", OpenIndexes[fileDesc].attrType1);
    //printf("%d >= %d\n", *(int*)value1, *(int*)temp_key);
    while(found_block_pos < key_num &&
          v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) > -1) {
      // Get next key after block
      found_block_pos++;
      memcpy(temp_key, block_data + found_block_pos*key_plus_ptr_size + sizeof(int),
             OpenIndexes[fileDesc].attrLength1);
    }
    //printf("FOUND BLOCK POS %d\n", found_block_pos);
    // Get block id
    int found_block_id = 0;
    memcpy(&found_block_id, block_data + found_block_pos*key_plus_ptr_size,
            sizeof(int));
    //printf("%d\n", found_block_id);
    // Before the recursive call
    block_data = NULL;
    CHK_BF_ERR_RECTRAV(BF_UnpinBlock(block));
    // Then call the same function with found_block_num as input block number
    //printf("RENINSIDEREC\n" );
    possible_block = rec_trav_insert(fileDesc, found_block_id, value1, value2);
    //printf("AFTER REC\n" );
    // If the returned struct's possible_block.nblock_id is not -1, that
    // means that a new block has been created in a lower level, so a key and a
    // pointer to it should be added in this block
    if (possible_block.nblock_id != -1) {
      CHK_BF_ERR_RECTRAV(BF_GetBlock(fd, block_id, block));
      block_data = BF_Block_GetData(block) + 4 + sizeof(int);

      // Calculate used space and check if a new key fits in the block
      int used_space = 4 + 2*sizeof(int) + key_num*key_plus_ptr_size;
      // If it fits, then insert it after the found_block_pos
      if (used_space + key_plus_ptr_size <= BF_BLOCK_SIZE) {
        // If the key to be inserted will go last, only insert it
        if (found_block_pos == key_num) {
          // Insert new key and pointer
          memcpy(block_data + found_block_pos*key_plus_ptr_size + sizeof(int),
                 possible_block.nblock_strt_key, OpenIndexes[fileDesc].attrLength1);
          memcpy(block_data + (found_block_pos + 1)*key_plus_ptr_size,
                 &possible_block.nblock_id, sizeof(int));
        }
        // Else save records that come after the new one
        else {
          int copy_block_num = key_num - found_block_pos;
          char* buffer = malloc(copy_block_num*key_plus_ptr_size);
          memcpy(buffer, block_data + found_block_pos*key_plus_ptr_size + sizeof(int),
                 copy_block_num*key_plus_ptr_size);

          // Insert new key and pointer
          memcpy(block_data + found_block_pos*key_plus_ptr_size + sizeof(int),
                 possible_block.nblock_strt_key, OpenIndexes[fileDesc].attrLength1);
          memcpy(block_data + (found_block_pos + 1)*key_plus_ptr_size,
                 &possible_block.nblock_id, sizeof(int));

          // Move all records that come after that 1 position to the right
          memcpy(block_data + (found_block_pos + 1)*key_plus_ptr_size + sizeof(int),
                 buffer, copy_block_num*key_plus_ptr_size);
          // Free buffer
          free(buffer);
        }
        // Increment the block's second value (number of keys in it) by one
        key_num++;
        memcpy(block_data - sizeof(int), &key_num, sizeof(int));

        // Set dirty and unpin block
        BF_Block_SetDirty(block);
        CHK_BF_ERR_RECTRAV(BF_UnpinBlock(block));

        // Free possible_block's nblock_strt_key and make nblock_id -1
        // so that we know that there are no blocks to insert at the upper level
        free(possible_block.nblock_strt_key);
        possible_block.nblock_id = -1;
      }
      // Else, create a new index block and distribute the keys between them
      else {
        int all_key_num = key_num + 1;
        // Find out which key to send to the upper level
        // The right block will have more keys if the key number is even
        int send_key_pos = all_key_num/2;

        // Update the key number of the original index block
        // send_key_pos number is also the new key number of the original block
        memcpy(block_data - sizeof(int), &send_key_pos, sizeof(int));

        // Create new index block
        BF_Block *new_block;
        BF_Block_Init(&new_block);
        CHK_BF_ERR_RECTRAV(BF_AllocateBlock(fd, new_block));
        int new_block_id = 0;
        CHK_BF_ERR_RECTRAV(BF_GetBlockCounter(fd, &new_block_id));
        new_block_id--;
        // Initialize index block with id
        char* new_block_data = BF_Block_GetData(new_block);
        char rid[4] = ".ib"; // It is an "index block"
        memcpy(new_block_data, rid, 4);
        new_block_data += 4;
        // Initialize index block with key number
        int new_key_num = 0;
        if (all_key_num % 2 == 1)
          // Splits in half
          new_key_num = send_key_pos;
        else
          // This block gets less keys
          new_key_num = send_key_pos - 1;
        memcpy(new_block_data, &new_key_num, sizeof(int));
        new_block_data += sizeof(int);

        // Allocate a buffer to split the keys
        char* buffer = malloc(all_key_num*key_plus_ptr_size + sizeof(int));
        // Copy first block number
        memcpy(buffer, block_data, sizeof(int));
        // Copy keys and block numbers to a buffer
        // TSEKARE AUTOOOO LATHOOOOOSS
        int copied = 0;
        for (int i = 0; i < all_key_num; i++) {
          if (i == found_block_pos) {
            memcpy(buffer + sizeof(int) + i*key_plus_ptr_size,
                   possible_block.nblock_strt_key,
                   OpenIndexes[fileDesc].attrLength1);
            memcpy(buffer + sizeof(int) + i*key_plus_ptr_size + OpenIndexes[fileDesc].attrLength1,
                   &possible_block.nblock_id,
                   sizeof(int));
            copied = 1;
          }
          else {
            memcpy(buffer + sizeof(int) + i*key_plus_ptr_size,
                   block_data + sizeof(int) + (i - copied)*key_plus_ptr_size,
                   key_plus_ptr_size);
          }
        }
        // Divide keys between the blocks
        memcpy(block_data, buffer, sizeof(int));
        int new_block_i = 0;
        for (int i = 0; i < all_key_num; i++) {
          if (i < send_key_pos) {
            memcpy(block_data + sizeof(int) + i*key_plus_ptr_size,
                   buffer + sizeof(int) + i*key_plus_ptr_size,
                   key_plus_ptr_size);
          }
          else if (i > send_key_pos) {
            memcpy(new_block_data + sizeof(int) + new_block_i*key_plus_ptr_size,
                   buffer + sizeof(int) + i*key_plus_ptr_size,
                   key_plus_ptr_size);
            new_block_i++;
          }
          else {
            // Just copy the first block pointer of the new index block
            memcpy(new_block_data,
                   buffer + (send_key_pos + 1)*key_plus_ptr_size,
                   sizeof(int));
            // And pass the output key value to the possible_block struct
            memcpy(possible_block.nblock_strt_key,
                   buffer + sizeof(int) + send_key_pos*key_plus_ptr_size,
                   OpenIndexes[fileDesc].attrLength1);
          }
        }
        // Pass the output block_id to the possible_block struct
        memcpy(&(possible_block.nblock_id),
               &new_block_id,
               sizeof(int));

        // Free buffer
        free(buffer);
        // Dirty and unpin blocks
        // Original data block
        BF_Block_SetDirty(block);
        CHK_BF_ERR_RECTRAV(BF_UnpinBlock(block));
        // New data block
        BF_Block_SetDirty(new_block);
        CHK_BF_ERR_RECTRAV(BF_UnpinBlock(new_block));
        // Destroy new block
        BF_Block_Destroy(&new_block);
      }
    // Else just return the same output (possible_block with nblock_id == -1)
    }
  }




  // Else, if it is a data block, that means that this is where the given
  // values should be inserted (end recursion)
  else if (strcmp(block_data, ".db") == 0) {
    //printf("IN DATA\n" );
    // Get number of records (consist of 2 attributes) in block
    block_data += 4;
    int record_num = 0;
    memcpy(&record_num, block_data, sizeof(int));
    block_data += 2*sizeof(int);
    // Check if the new record fits in the block
    int record_size = OpenIndexes[fileDesc].attrLength1 +
                      OpenIndexes[fileDesc].attrLength2;
    int used_space = 4 + 2*sizeof(int) + record_num*record_size;
    // If it fits, then insert it
    if (used_space + record_size <= BF_BLOCK_SIZE) {
      // Get first record key value
      memcpy(temp_key, block_data, OpenIndexes[fileDesc].attrLength1);
      // Find out where its position should be
      int move_pos = 0;
      while (move_pos < record_num &&
            v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) >= 0) {
        move_pos++;
        // Move on to the next key
        memcpy(temp_key, block_data + move_pos*record_size,
               OpenIndexes[fileDesc].attrLength1);
      }
      // If the record to be inserted will go last, only insert it
      if (move_pos == record_num) {
        memcpy(block_data + move_pos*record_size,
               value1, OpenIndexes[fileDesc].attrLength1);
        memcpy(block_data + move_pos*record_size + OpenIndexes[fileDesc].attrLength1,
               value2, OpenIndexes[fileDesc].attrLength2);
      }
      // Else move the records after it by one position
      else {
        // Save records that come after the new one
        int copy_rec_num = record_num - move_pos;
        char* buffer = malloc(copy_rec_num*record_size);
        memcpy(buffer, block_data + move_pos*record_size, copy_rec_num*record_size);

        // Insert new record
        memcpy(block_data + move_pos*record_size,
               value1, OpenIndexes[fileDesc].attrLength1);
        memcpy(block_data + move_pos*record_size + OpenIndexes[fileDesc].attrLength1,
               value2, OpenIndexes[fileDesc].attrLength2);

        // Move all records that come after that 1 position to the right
        memcpy(block_data + (move_pos + 1)*record_size,
               buffer, copy_rec_num*record_size);

        // Free buffer
        free(buffer);
      }
      // Increment the block's second value (number of records in it) by one
      record_num++;
      memcpy(block_data - 2*sizeof(int), &record_num, sizeof(int));

      // Set dirty and unpin block
      BF_Block_SetDirty(block);
      CHK_BF_ERR_RECTRAV(BF_UnpinBlock(block));

      possible_block.nblock_id = -1;
    }


    // Else, create a new data block and distribute the records between them in
    // the best way possible (records with the same key value should not be
    // separated)
    else {
      // First copy all records to a buffer that has enough space for one more
      // Find out where the new record's position should be in the buffer
      int move_pos = 0;
      memcpy(temp_key,
             block_data + move_pos*record_size,
             OpenIndexes[fileDesc].attrLength1);
      while (move_pos < record_num &&
            v_cmp(OpenIndexes[fileDesc].attrType1, value1, temp_key) >= 0) {
        move_pos++;
        memcpy(temp_key,
               block_data + move_pos*record_size,
               OpenIndexes[fileDesc].attrLength1);
      }
      // Allocate buffer to split the blocks
      int all_rec_num = record_num + 1;
      char* buffer= malloc(all_rec_num * record_size);
      // Copy records to buffer
      int copied = 0;
      for (int i = 0; i < all_rec_num; i++) {
        if (i == move_pos) {
          memcpy(buffer + i*record_size,
                 value1,
                 OpenIndexes[fileDesc].attrLength1);
          memcpy(buffer + i*record_size + OpenIndexes[fileDesc].attrLength1,
                 value2,
                 OpenIndexes[fileDesc].attrLength2);
          copied = 1;
        }
        else {
          memcpy(buffer + i*record_size,
                 block_data + (i - copied)*record_size,
                 record_size);
        }
      }
      // Allocate space for another key
      void* temp_key2 = malloc(OpenIndexes[fileDesc].attrLength1);
      // Check if the last record of the left block and the first record of the
      // new, right block have the same key value
      int first_right_rec_pos = 0;
      if (all_rec_num % 2 == 1)
        first_right_rec_pos = all_rec_num/2 + 1;
      else
        first_right_rec_pos = all_rec_num/2;

      memcpy(temp_key,
             buffer + (first_right_rec_pos - 1)*record_size,
             OpenIndexes[fileDesc].attrLength1);
      memcpy(temp_key2,
             buffer + (first_right_rec_pos - 1)*record_size,
             OpenIndexes[fileDesc].attrLength1);
      // If they do, then split the records between the blocks the best way
      // possible without splitting records with the same key
      int left_rec_num = 0, right_rec_num = 0, middle_key_num = 0, less_than_num = 0, greater_than_num = 0;
      if (v_cmp(OpenIndexes[fileDesc].attrType1, temp_key, temp_key2) == 0) {
        // Count how many records have the same key as them and how many less
        for(int i = 0; i < all_rec_num; i++) {
          memcpy(temp_key2,
                 buffer + i*record_size,
                 OpenIndexes[fileDesc].attrLength1);
          if (v_cmp(OpenIndexes[fileDesc].attrType1, temp_key2, temp_key) == -1)
            less_than_num++;
          else if (v_cmp(OpenIndexes[fileDesc].attrType1, temp_key2, temp_key) == 0)
            middle_key_num++;
          else
            greater_than_num++;
        }
        if (less_than_num <= greater_than_num) {
          left_rec_num = less_than_num + middle_key_num;
          right_rec_num = greater_than_num;
        }
        else {
          left_rec_num = less_than_num;
          right_rec_num = greater_than_num + middle_key_num;
        }
      }
      // Else divide normally
      else {
          left_rec_num = first_right_rec_pos;
          right_rec_num = all_rec_num - first_right_rec_pos;
      }

      // Create new data block
      BF_Block *new_block;
      BF_Block_Init(&new_block);
      CHK_BF_ERR_RECTRAV(BF_AllocateBlock(fd, new_block));
      int new_block_id = 0;
      CHK_BF_ERR_RECTRAV(BF_GetBlockCounter(fd, &new_block_id));
      new_block_id--;
      // Initialize index block with id
      char* new_block_data = BF_Block_GetData(new_block);
      char rid[4] = ".db"; // It is an "index block"
      memcpy(new_block_data, rid, 4);
      new_block_data += 4;
      // Initialize number of records
      memcpy(new_block_data, &right_rec_num, sizeof(int));
      new_block_data += sizeof(int);
      // The block after this block is the block after the original
      memcpy(new_block_data, block_data - sizeof(int), sizeof(int));
      new_block_data += sizeof(int);

      // Update the original block's record number
      memcpy(block_data - 2*sizeof(int), &left_rec_num, sizeof(int));
      // The block after the original block is this block
      memcpy(block_data - sizeof(int), &new_block_id, sizeof(int));

      // Copy records from buffer
      int new_block_i = 0;
      for (int i = 0; i < all_rec_num; i++) {
        if (i < first_right_rec_pos) {
          memcpy(block_data + i*record_size,
                 buffer + i*record_size,
                 record_size);
        }
        else if (i >= first_right_rec_pos) {
          memcpy(new_block_data + new_block_i*record_size,
                 buffer + i*record_size,
                 record_size);
          new_block_i++;
        }
      }

      // Pass the output values to the possible_block struct
      // First create a RecTravOut struct
      possible_block.nblock_strt_key = malloc(OpenIndexes[fileDesc].attrLength1);

      memcpy(possible_block.nblock_strt_key,
             new_block_data,
             OpenIndexes[fileDesc].attrLength1);
      memcpy(&(possible_block.nblock_id),
             &new_block_id,
             sizeof(int));
      possible_block.error = 0;

      // Free temp_key2
      free(temp_key2);
      // Free buffer
      free(buffer);
      // Dirty and unpin blocks
      // Original data block
      BF_Block_SetDirty(block);
      CHK_BF_ERR_RECTRAV(BF_UnpinBlock(block));
      // New data block
      BF_Block_SetDirty(new_block);
      CHK_BF_ERR_RECTRAV(BF_UnpinBlock(new_block));
      // Destroy new block
      BF_Block_Destroy(&new_block);
    }
  }

  free(temp_key);
  BF_Block_Destroy(&block);

  return possible_block;

}
