#include <stdio.h>
#include <string.h>

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



int AM_errno = AME_OK;

void AM_Init() {
  // Initialize BF part
  CHK_BF_ERR(BF_Init(LRU));
  // Initialize global
  for(int i = 0; i < MAXOPENFILES; i++)
    OpenIndexes[i] = filemeta_init(OpenIndexes[i]);

  for(int i = 0; i < MAXSCANS; i++)
    OpenSearches[i] = searchdata_init(OpenSearches[i]);

	return;
}


int AM_CreateIndex(char *fileName,
	               char attrType1,
	               int attrLength1,
	               char attrType2,
	               int attrLength2) {

	// Check for possible attrType and attrLength input errors
  // Attribute 1
	if (attrType1 == INTEGER || attrType1 == FLOAT) {
		if (attrLength1 != 4) {
      AM_errno = AME_CREATE_INPUT_ERROR;
		  return AME_CREATE_INPUT_ERROR;
    }
	}
  else if (attrType1 == STRING) {
    if (attrLength1 < 1 || attrLength1 > 255) {
      AM_errno = AME_CREATE_INPUT_ERROR;
		  return AME_CREATE_INPUT_ERROR;
    }
  }
  else {
    AM_errno = AME_CREATE_INPUT_ERROR;
    return AME_CREATE_INPUT_ERROR;
  }
  // Attribute 2
	if (attrType2 == INTEGER || attrType2 == FLOAT) {
    if (attrLength2 != 4) {
      AM_errno = AME_WRONG_ATTR_LENGTH;
		  return AME_WRONG_ATTR_LENGTH;
    }
	}
  else if (attrType2 == STRING) {
    if (attrLength2 < 1 || attrLength2 > 255) {
      AM_errno = AME_WRONG_ATTR_LENGTH;
		  return AME_WRONG_ATTR_LENGTH;
    }
  }
  else {
    AM_errno = AME_CREATE_INPUT_ERROR;
    return AME_CREATE_INPUT_ERROR;
  }

	// Create and open file
	CHK_BF_ERR(BF_CreateFile(filename));
	int fd = 0;
	CHK_BF_ERR(BF_OpenFile(filename, &fd));

	// Allocate the file's first block
  BF_Block *block;
  BF_Block_Init(&block);
  CHK_BF_ERR(BF_AllocateBlock(fd, block));

  // Initialize it with metadata
  char* block_data = BF_Block_GetData(block);
	// Index file id (.if) (space for \0 at the end for strcmp)
  char id[4] = ".if";
  // TSEKAREEEEE AUTOOOOO +4 gt IPARXEI KAI TO \0 STO TELOS
  memcpy(block_data, id, 4);
	block_data += 4;
	// Then save the types and lengths of the attributes
	memcpy(block_data, &attrType1, 1);
	block_data++;
	memcpy(block_data, &attrLength1, sizeof(int));
	block_data += sizeof(int);
	memcpy(block_data, &attrType2, 1);
	block_data++;
	memcpy(block_data, &attrLength2, sizeof(int));
	block_data += sizeof(int);
	// Finally place -1 values where the tree root and first data block numbers
	// will be stored
  int neg1 = -1;
	memcpy(block_data, &neg1, sizeof(int));
	block_data += sizeof(int);
	memcpy(block_data, &neg1, sizeof(int));

	// Dirty and unpin
  BF_Block_SetDirty(block);
  CHK_BF_ERR(BF_UnpinBlock(block));
  // Destroy block and close file
  BF_Block_Destroy(&block);
  CHK_BF_ERR(BF_CloseFile(fd));

  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
	// Check if index file is open
  int i = 0;
  while(i < MAXOPENFILES) {
    if (strcmp(OpenIndexes[i].fileName, fileName) == 0) {
      AM_errno = AME_CANNOT_DESTROY_INDEX_OPEN;
      return AME_CANNOT_DESTROY_INDEX_OPEN;
    }
    i++;
  }
  // If not, delete file
  // MPOREI NA THELEI ./ (GIA DIR)
  remove(filename);
  return AME_OK;
}


int AM_OpenIndex (char *fileName) {
  // Search for the first empty space to store the opened file
  int save_index = 0;
  while (save_index < MAXOPENFILES && OpenIndexes[save_index].fd != -1)
    save_index++;
  // If there is no space
  if (save_index == MAXOPENFILES) {
    AM_errno = AME_NO_SPACE_FOR_INDEX;
    return AME_NO_SPACE_FOR_INDEX;
  }

  // Open file
  int fd = 0;
  CHK_BF_ERR(BF_OpenFile(fileName, &fd));
  // Check if there is a block in the file
  int block_num;
  CHK_BF_ERR(BF_GetBlockCounter(fd, &block_num));
  if (block_num == 0) {
    AM_errno = AME_NOT_INDEX_FILE;
    return AME_NOT_INDEX_FILE;
  }

  // Then check if it is an index file
  BF_Block *block;
  BF_Block_Init(&block);
  // There should be an ".if" at the start of the first block
  CHK_BF_ERR(BF_GetBlock(fd, 0, block));
  char* block_data = BF_Block_GetData(block);
  if (strcmp(block_data, ".if") != 0) {
    AM_errno = AME_NOT_INDEX_FILE;
    return AME_NOT_INDEX_FILE;
  }

  // Save file decriptor and filename
  OpenIndexes[save_index].fd = fd;
  // MPOREI LATHOS
  char* fname = malloc(sizeof(fileName));
  strcpy(fname, fileName);
  OpenIndexes[save_index].fileName = fname;
  // Save file metadata in the available FileMeta struct
  memcpy(&(OpenIndexes[save_index].attrType1), block_data, 1);
  block_data++;
  memcpy(&(OpenIndexes[save_index].attrLength1), block_data, sizeof(int));
  block_data += sizeof(int);
  memcpy(&(OpenIndexes[save_index].attrType2), block_data, 1);
  block_data++;
  memcpy(&(OpenIndexes[save_index].attrLength2), block_data, sizeof(int));
  block_data += sizeof(int);
  memcpy(&(OpenIndexes[save_index].rootBlockNum), block_data, sizeof(int));
  block_data += sizeof(int);
  memcpy(&(OpenIndexes[save_index].dataBlockNum), block_data, sizeof(int));

  // Unpin and destroy block
  CHK_BF_ERR(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

	return AME_OK;
}


int AM_CloseIndex (int fileDesc) {
  // First check if there are any open searches on this file index
  for (int i = 0; i < MAXSCANS; i++) {
    if (OpenSearches[i].fileDesc == fileDesc) {
      AM_errno = AME_CANNOT_CLOSE_SEARCH_OPEN;
      return AME_CANNOT_CLOSE_SEARCH_OPEN;
    }
  }
  // Free allocated space used to store the filename
  free(OpenIndexes[fileDesc].fileName);
  // Reset the index value
  OpenIndexes[fileDesc] = filemeta_init(OpenIndexes[fileDesc]);
	return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  // Check if file is open in OpenIndexes[fileDesc]
  int fd = OpenIndexes[fileDesc].fd;
  if (fd == -1) {
    AM_errno = AME_INDEX_FILE_NOT_OPEN;
    return AME_INDEX_FILE_NOT_OPEN;
  }

  // Check if a B+ tree root exists
  // If not, initialize B+ index tree with the first record
  // (create tree root and the first data block)
  if (OpenIndexes[fileDesc].rootBlockNum = -1) {
    // Allocate block for the first data block
    BF_Block *block;
    BF_Block_Init(&block);
    CHK_BF_ERR(BF_AllocateBlock(fd, block));
    // Initialize data block with id, record number (1), next data block (-1),
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
    // There is no next data block (-1)
    temp_i = -1;
    memcpy(block_data, &temp_i, sizeof(int));
    block_data += sizeof(int);
    // Copy input values
    memcpy(block_data, &value1, OpenIndexes[fileDesc].attrLength1);
    block_data += OpenIndexes[fileDesc].attrLength1;
    memcpy(block_data, &value2, OpenIndexes[fileDesc].attrLength2);

    // Get block number of the data block and save it in OpenIndexes
    int block_num = 0;
    CHK_BF_ERR(BF_GetBlockCounter(fileDesc, &block_num));
    block_num--;
    OpenIndexes[fileDesc].dataBlockNum = block_num;

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

    // Get block number of the root and save it in OpenIndexes
    CHK_BF_ERR(BF_GetBlockCounter(fd, &block_num));
    block_num--;
    OpenIndexes[fileDesc].rootBlockNum = rblock_num;

    // Set dirty, unpin and destroy block
    BF_Block_SetDirty(block);
    CHK_BF_ERR(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
  }
  // Else if tree exists, insert record (value1, value2), according to
  // B+ tree algorithm
  else {
    // Initialize block
    BF_Block *tree_block;
    BF_Block_Init(&tree_block);
    // First get tree root data
    CHK_BF_ERR(BF_GetBlock(fd, OpenIndexes[fileDesc].rootBlockNum, tree_block));
    char* tree_data = BF_Block_GetData(tree_block);

    // Then, only if input key (value1) is less than the first key of the
    // root block, and the first pointer of the root does not point to a
    // block (-1), make a new data block, place the record in it and point to it

    // Get first block number 
    tree_data += 4 + sizeof(int);
    int fblock_num = 0;
    memcpy(fblock_num, tree_data, sizeof(int));
    // Get first key
    tree_data += sizeof(int);
    void* fkey = (void *)malloc(OpenIndexes[fileDesc].attrLength1);
    memcpy(fkey, tree_data, OpenIndexes[fileDesc].attrLength1);

    if (fblock_num == -1 &&
        v_cmp(OpenIndexes[fileDesc].attrType1, value1, fkey) == -1) {
      // Allocate data block
      BF_Block *block;
      BF_Block_Init(&block);
      CHK_BF_ERR(BF_AllocateBlock(fd, block));
      // Initialize data block with id, record number (1), next data block (-1),
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
      // Next data block is the current first block
      temp_i = OpenIndexes[fileDesc].dataBlockNum;
      memcpy(block_data, &temp_i, sizeof(int));
      block_data += sizeof(int);
      // Copy input values
      memcpy(block_data, &value1, OpenIndexes[fileDesc].attrLength1);
      block_data += OpenIndexes[fileDesc].attrLength1;
      memcpy(block_data, &value2, OpenIndexes[fileDesc].attrLength2);

      // Get block number of the data block and save it in OpenIndexes
      int block_num = 0;
      CHK_BF_ERR(BF_GetBlockCounter(fileDesc, &block_num));
      block_num--;
      OpenIndexes[fileDesc].dataBlockNum = block_num;

      // Set dirty, unpin and destroy block
      BF_Block_SetDirty(block);
      CHK_BF_ERR(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
    }
    // Else APLA KALESE REC
    else {

    }

    // Set dirty, unpin and destroy block
    BF_Block_SetDirty(tree_block);
    CHK_BF_ERR(BF_UnpinBlock(tree_block));
    BF_Block_Destroy(&tree_block);
  }
  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) { //fileDesc isthe location in OpenIndexes

  // Check if file is open in OpenIndexes[fileDesc]
  int fd = OpenIndexes[fileDesc].fd;
  if (fd == -1) {
    AM_errno = AME_INDEX_FILE_NOT_OPEN;
    return AME_INDEX_FILE_NOT_OPEN;
  }
  //Check if there is a root in the file
  if (OpenIndexes[fileDesc].rootBlockNum = -1) {
    AM_errno = ROOT_NOT_EXIST;
    return AME_ROOT_NOT_EXIST;
  }

  for(int scanDesc = 0; scanDesc < MAXSCANS; i++) {//search for location in OpenSearches
    OpenSearches[scanDesc].fileDesc==-1;
    break;
  }

  //Check if there is location in OpenSearches
  if(scanDesc==MAXSCANS){
    AM_errno = AME_NO_SPACE_FOR_SEARCH;
    return AME_NO_SPACE_FOR_SEARCH;
  }

//////////////sizes///////////////////
  int id_sz=4;
  int key_num_sz=sizeof(int);
  int pointer_sz=sizeof(int);
  int key_sz=OpenIndexes[fileDesc].attrLength1;
/////////////////////////////////////



  BF_Block *block;
  BF_Block_Init(&block);
  int fd=OpenIndexes[fileDesc].fd;
  char* data;

  char* id=(char*)malloc(id_sz);
  char* key_number=(char*)malloc(key_num_sz);
  char* pointer=(char*)malloc(pointer_sz);
  char* key=(char*)malloc(key_sz);

  switch (op) {
    case EQUAL:
      While (1){
        CHK_BF_ERR(BF_GetBlock(fd,OpenIndexes[fileDesc].rootBlockNum,block));
        data=BF_Block_GetData(block);
        memcpy(id,data,id_sz);
        if(v_cmp('c',id,".ib")==0){  //search for next indexblock or datablock
            memcpy(key_number,data+id_sz,key_num_sz);
            memcpy(pointer,data+id_sz+key_num_sz,pointer_sz);
            for(int i=0;i<atoi(key_number),i++){
                memcpy(key,data + id_sz + key_num_sz + (i+1)*pointer_sz + i*key_sz,key_sz);
                if(v_cmp(OpenIndexes[fileDesc].attrType1,key))
            }
        }
        else{//we have a datablock so we serach for key

        }
        CHK_BF_ERR(BF_UnpinBlock(block));
      }
      break;
    case NOT_EQUAL:
      break;
    case LESS_THAN:
      break;
    case GREATER_THAN:
      break;
    case LESS_THAN_OR_EQUAL:
      break;
    case GREATER_THAN_OR_EQUAL:
      break;
    default:
    free(id);
    free(key_number);
    free(pointer);
    free(key);
    AM_errno = AME_INVALID_OP;
    return AME_INVALID_OP;
  }




  CHK_BF_ERR(BF_GetBlock(fd,OpenIndexes[fileDesc].dataBlockNum,block));
  BF_UnpinBlock
  BF_GetBlock

  return scanDesc;
}


void *AM_FindNextEntry(int scanDesc) { //loaction in searchdata

}


int AM_CloseIndexScan(int scanDesc) {

  if(OpenSearches[scanDesc].fileDesc==-1) { //check if in OpenIndexes
    AM_errno = AME_CANNOT_DESTROY_SEARCH_OPEN;
    return AME_CANNOT_DESTROY_SEARCH_OPEN;
  }
  else //if it is initialize again the data
      OpenSearches[scanDesc] = searchdata_init(OpenSearches[scanDesc]);
  return AME_OK;
}


void AM_PrintError(char *errString) {
  // APLA PRINT TA FTIAGMENA STO .h
}

void AM_Close() {
  //CLOSE KAI TI ALLO DEN KSERW
  BF_Close();
}
