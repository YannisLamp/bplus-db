#include <string.h>
#include "defn.h"

// Compares variables depending on their type (input v_type)
// Output similar to strcmp (only with -2 output for input errors)
//THELEI TESTING OPWSDIPOTE
int v_cmp(char v_type, void* value1, void* value2) {
  if (v_type == INTEGER) {
    if ((*(int*) value1) < (*(int*) value2))
      return -1;
    else if ((*(int*) value1) < (*(int*) value2))
      return 1;
    else
      return 0;
  }
  else if (v_type == FLOAT) {
    if ((*(float *)value1) < (*(float *)value2))
      return -1;
    else if ((*(float *)value1) < (*(float *)value2))
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

RecTravOut rec_trav_insert(int fileDesc, int block_num, void *value1, void *value2) {
  // Initialize block and get file descriptor
  BF_Block *block;
  BF_Block_Init(&block);
  int fd = OpenIndexes[fileDesc].fd;
  // Get block data
  CHK_BF_ERR(BF_GetBlock(fd, block_num, block));
  char* block_data = BF_Block_GetData(tree_block);

  // First check if current block is an index block or a data block
  // If it is an index block, continue traversal (recursion)
  // (in order to find the right data block for the values to be inserted into)
  if (strcmp(block_data, ".ib") == 0) {
    // Get number of keys in block
    block_data += 4;
    int block_key_num = 0;
    memcpy(&block_key_num, block_data, sizeof(int));
    // Get first key
    block_data += 2*sizeof(int);
    void* curr_key = (void *)malloc(OpenIndexes[fileDesc].attrLength1);
    memcpy(curr_key, block_data, OpenIndexes[fileDesc].attrLength1);

    // Then try to find next block
    while(block_key_num < 1 &&
          v_cmp(OpenIndexes[fileDesc].attrType1, value1, curr_key) > -1) {
      // Move on to the next key
      block_data += OpenIndexes[fileDesc].attrLength1 + sizeof(int);
      block_key_num--;
      memcpy(curr_key, block_data, OpenIndexes[fileDesc].attrLength1);
    }
    // Only if it is the last key go to right pointer
    if (v_cmp(OpenIndexes[fileDesc].attrType1, value1, curr_key) > -1)
      block_data += OpenIndexes[fileDesc].attrLength1;
    // Else normally go to left
    else
      block_data -= sizeof(int);

    free(curr_key);
    // Unpin block
    CHK_BF_ERR(BF_UnpinBlock(root_block));

    // Then call the same function with found_block_num as input block number
    int found_block_num = 0;
    memcpy(&found_block_num, block_data, sizeof(int));
    RecTravOut possible_block = rec_trav_insert(fileDesc, found_block_num, value1, value2);

    // If the returned struct's possible_block.

  }
  // Else, if it is a data block, that means that this is where the given
  // values should be inserted (end recursion)
  else if (strcmp(block_data, ".db") == 0) {

  }

  BF_Block_Destroy(&root_block);

}
