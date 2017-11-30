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

  // Initialize global
  for(int i = 0; i < MAXOPENFILES; i++)
    OpenIndexes[i]=filemeta_init(OpenIndexes[i]);

  for(int i = 0; i < MAXSCANS; i++)
    OpenSearches[i]=searchdata_init(OpenSearches[i]);

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
	int fileDesc = 0;
	CHK_BF_ERR(BF_OpenFile(filename, &fileDesc));

	// Allocate the file's first block
  BF_Block *block;
  BF_Block_Init(&block);
  CHK_BF_ERR(BF_AllocateBlock(fileDesc, block));

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
  CHK_BF_ERR(BF_CloseFile(fileDesc));

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
  while (save_index < MAXOPENFILES && OpenIndexes[save_index].fileDesc != -1)
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
  OpenIndexes[save_index].fileDesc = fd;
  //AUTOOOOOO
  malloc
  OpenIndexes[save_index].fileName =
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
  // AN KANW MALLOC GIA TO FILENAME EDW FREE
  // AN APOFASISW AN THA ALLW TIS TIMES TWN ALLWN


	return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  // Check if file is open in OpenIndexes[fileDesc]
  int fd = OpenIndexes[fileDesc].fileDesc;
  if (fd == -1) {
    AM_errno = AME_INDEX_FILE_NOT_OPEN;
    return AME_INDEX_FILE_NOT_OPEN;
  }

  // Then check if a B+ tree root exists
  // If not, initialize B+ index tree with the first record
  // (create tree root and data block)
  if (OpenIndexes[fileDesc].rootBlockNum = -1) {
    // Allocate block for root
    BF_Block *rblock;
    BF_Block_Init(&rblock);
    char* rblock_data = NULL;
    CHK_BF_ERR(BF_AllocateBlock(fd, rblock));
    // Initialize root block with id and key_number (1)
    rblock_data = BF_Block_GetData(rblock);
    // It is an "index block"
    char rid[4] = ".ib";
    memcpy(rblock_data, rid, 4);
    rblock_data += 4;
    // One key will be inserted
    int temp_i = 1;
    memcpy(rblock_data, &temp_i, sizeof(int));
    rblock_data += sizeof(int);

    // Get block number of the root and save it in OpenIndexes
    int rblock_num;
    CHK_BF_ERR(BF_GetBlockCounter(fileDesc, &rblock_num));
    rblock_num--;
    OpenIndexes[fileDesc].rootBlockNum = rblock_num;

    // Allocate block for the first data block
    BF_Block *dblock;
    BF_Block_Init(&dblock);
    char* dblock_data = NULL;
    CHK_BF_ERR(BF_AllocateBlock(fd, dblock));
    // Initialize data block with id, record number (1), next data block (-1),
    // and finally the record (value1, value2)
    dblock_data = BF_Block_GetData(dblock);
    // It is a "data block"
    char did[4] = ".db";
    memcpy(dblock_data, did, 4);
    dblock_data += 4;
    // One record will be inserted
    temp_i = 1;
    memcpy(dblock_data, &temp_i, sizeof(int));
    dblock_data += sizeof(int);
    // There is no next data block (-1)
    temp_i = -1;
    memcpy(dblock_data, &temp_i, sizeof(int));
    dblock_data += sizeof(int);
    // Copy input values
    memcpy(dblock_data, &value1, OpenIndexes[fileDesc].attrLength1);
    dblock_data += OpenIndexes[fileDesc].attrLength1;
    memcpy(dblock_data, &value2, OpenIndexes[fileDesc].attrLength2);

    // Get block number of the data block,
    // save it in OpenIndexes and finalize initialization of the root block
    int dblock_num;
    CHK_BF_ERR(BF_GetBlockCounter(fileDesc, &dblock_num));
    dblock_num--;
    OpenIndexes[fileDesc].dataBlockNum = dblock_num;
    // Block number before the first key does not exist yet (-1)
    temp_i = -1;
    memcpy(rblock_data, &temp_i, sizeof(int));
    rblock_data += sizeof(int);
    // Finally save key, then data block number in the root
    memcpy(rblock_data, &value1, OpenIndexes[fileDesc].attrLength1);
    rblock_data += OpenIndexes[fileDesc].attrLength1;
    memcpy(rblock_data, &dblock_num, sizeof(int));

    // Unpin and destroy blocks
    CHK_BF_ERR(BF_UnpinBlock(rblock));
    BF_Block_Destroy(&rblock);
    CHK_BF_ERR(BF_UnpinBlock(dblock));
    BF_Block_Destroy(&dblock);
  }
  // Else if tree exists, insert record (value1, value2), according to
  // B+ tree algorithm
  else {
    
  }



  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {

  // Check if file is open in OpenIndexes[fileDesc]
  int fd = OpenIndexes[fileDesc].fileDesc;
  if (fd == -1) {
    AM_errno = AME_INDEX_FILE_NOT_OPEN;
    return AME_INDEX_FILE_NOT_OPEN;
  }
  //Check if there is a root in the file
  if (OpenIndexes[fileDesc].rootBlockNum = -1) {
    AM_errno = ROOT_NOT_EXIST;
    return AME_ROOT_NOT_EXIST;
  }

  return AME_OK;
}


void *AM_FindNextEntry(int scanDesc) {

}


int AM_CloseIndexScan(int scanDesc) {
  return AME_OK;
}


void AM_PrintError(char *errString) {
  // APLA PRINT TA FTIAGMENA STO .h
}

void AM_Close() {

}
