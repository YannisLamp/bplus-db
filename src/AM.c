#include <stdio.h>
#include <stdlib.h>
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

  #define CHK_BF_ERR_NULL(call)        \
    {                                  \
      BF_ErrorCode code = call;        \
      if (code != BF_OK) {             \
        BF_PrintError(code);           \
  			AM_errno = AME_BF_CALL_ERROR;  \
        return NULL;                   \
      }                                \
    }

FileMeta OpenIndexes[MAXOPENFILES];
SearchData OpenSearches[MAXSCANS];

int AM_errno = AME_OK;


void AM_Init() {
  // Initialize BF part
  BF_Init(LRU);

  // Initialize global
  for(int i = 0; i < MAXOPENFILES; i++)
    OpenIndexes[i] = filemeta_init(OpenIndexes[i]);
  for(int i = 0; i < MAXSCANS; i++)
    OpenSearches[i] = searchdata_init(OpenSearches[i]);
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
      AM_errno = AME_CREATE_INPUT_ERROR;
		  return AME_CREATE_INPUT_ERROR;
    }
	}
  else if (attrType2 == STRING) {
    if (attrLength2 < 1 || attrLength2 > 255) {
      AM_errno = AME_CREATE_INPUT_ERROR;
		  return AME_CREATE_INPUT_ERROR;
    }
  }
  else {
    AM_errno = AME_CREATE_INPUT_ERROR;
    return AME_CREATE_INPUT_ERROR;
  }

	// Create and open file
	CHK_BF_ERR(BF_CreateFile(fileName));
	int fd = 0;
	CHK_BF_ERR(BF_OpenFile(fileName, &fd));

	// Allocate the file's first block
  BF_Block *block;
  BF_Block_Init(&block);
  CHK_BF_ERR(BF_AllocateBlock(fd, block));

  // Initialize it with metadata
  char* block_data = BF_Block_GetData(block);
	// Index file id (.if) (space for \0 at the end for strcmp)
  char id[4] = ".if";
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
    if (OpenIndexes[i].fd != -1 &&
        strcmp(OpenIndexes[i].fileName, fileName) == 0) {
      AM_errno = AME_CANNOT_DESTROY_INDEX_OPEN;
      return AME_CANNOT_DESTROY_INDEX_OPEN;
    }
    i++;
  }
  // If not, delete file
  remove(fileName);
  return AME_OK;
}

// GIA TESTING EKTIPWNEI AKAIRAIOUS APO
//print_check(eAentry);
int print_check(int index) {
  int fd = OpenIndexes[index].fd;
  if (fd == -1) {
    AM_errno = AME_INDEX_FILE_NOT_OPEN;
    return AME_INDEX_FILE_NOT_OPEN;
  }
  // Then check if it is an index file
  BF_Block *block;
  BF_Block_Init(&block);
  // There should be an ".if" at the start of the first block

  int next_block = OpenIndexes[index].dataBlockNum;

  while (next_block != -1) {
    CHK_BF_ERR(BF_GetBlock(fd, next_block, block));
    char* block_data = BF_Block_GetData(block);
    printf("NEWBLOCK\n");
    block_data += 4;
    int rec_num = 0;
    memcpy(&rec_num, block_data, sizeof(int));
    block_data += 2*sizeof(int);
    int record_size = OpenIndexes[index].attrLength1 + OpenIndexes[index].attrLength2;
    for (int i = 0; i <rec_num;i++) {
      int out = 0;
      memcpy(&out, block_data + i*record_size, sizeof(int));
      printf("%d\n", out);
    }
    memcpy(&next_block, block_data - sizeof(int), sizeof(int));
    // Unpin and destroy block
    CHK_BF_ERR(BF_UnpinBlock(block));
  }

  BF_Block_Destroy(&block);
	return index;
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
  int fd;
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

  block_data += 4;
  // Save file decriptor and filename
  OpenIndexes[save_index].fd = fd;
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

	return save_index;
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

  // Check if there is an ongoing search in the same file
  for(int i = 0; i < MAXSCANS; i++) {
    if (OpenSearches[i].fileDesc != -1) {
      if (strcmp(OpenIndexes[OpenSearches[i].fileDesc].fileName,
                 OpenIndexes[fileDesc].fileName) == 0) {
        AM_errno = AME_CANNOT_INSERT_SEARCH_OPEN;
        return AME_CANNOT_INSERT_SEARCH_OPEN;
      }
    }
  }

  int ret_value = AME_OK;
  // Initialize block
  BF_Block *block;
  BF_Block_Init(&block);
  // Check if a B+ tree root exists
  // If not, initialize B+ index tree with the first record
  // (create tree root and the first data block)
  if (OpenIndexes[fileDesc].rootBlockNum == -1) {
    ret_value = init_bptree(fileDesc, value1, value2);

    // Change Metadata block
    CHK_BF_ERR(BF_GetBlock(fd, 0, block));
    char* meta_data = BF_Block_GetData(block);
    meta_data += 4 + 1 + sizeof(int) + 1 + sizeof(int);
    // Insert root block number
    memcpy(meta_data, &OpenIndexes[fileDesc].rootBlockNum, sizeof(int));

    // Set dirty and unpin metadata block
    BF_Block_SetDirty(block);
    CHK_BF_ERR(BF_UnpinBlock(block));
  }
  // Else if tree exists, insert record (value1, value2), according to
  // B+ tree algorithm
  else {
    // First get tree root data
    CHK_BF_ERR(BF_GetBlock(fd, OpenIndexes[fileDesc].rootBlockNum, block));
    char* root_data = BF_Block_GetData(block);
    root_data += 4;
    // Get first block id
    root_data += sizeof(int);
    int first_block_id = 0;
    memcpy(&first_block_id, root_data, sizeof(int));
    // Get first key
    root_data += sizeof(int);
    void* curr_key = malloc(OpenIndexes[fileDesc].attrLength1);
    memcpy(curr_key, root_data, OpenIndexes[fileDesc].attrLength1);
    // Unpin root block
    root_data = NULL;
    CHK_BF_ERR(BF_UnpinBlock(block));

    // Then, only if input key (value1) is less than the first key of the
    // root block, and the first pointer of the root does not point to a
    // block (-1), make a new data block, place the record in it and point to it
    if (first_block_id == -1 &&
        v_cmp(OpenIndexes[fileDesc].attrType1, value1, curr_key) == -1) {
      // Call create_leftmost_block to create the new block, insert the new
      // record in it and change the OpenIndexes[fileDesc]
      ret_value = create_leftmost_block(fileDesc, value1, value2);

      // Then change the root block so that it points to it
      // (insert the leftmost block id in the root block)
      CHK_BF_ERR(BF_GetBlock(fd, OpenIndexes[fileDesc].rootBlockNum, block));
      char* root_data = BF_Block_GetData(block);
      root_data += 4 + sizeof(int);
      memcpy(root_data, &OpenIndexes[fileDesc].dataBlockNum, sizeof(int));

      // Set dirty and unpin root block
      BF_Block_SetDirty(block);
      root_data = NULL;
      CHK_BF_ERR(BF_UnpinBlock(block));

      // Change Metadata block
      CHK_BF_ERR(BF_GetBlock(fd, 0, block));
      char* meta_data = BF_Block_GetData(block);
      meta_data += 4 + 1 + sizeof(int) + 1 + sizeof(int) + sizeof(int);
      memcpy(meta_data, &OpenIndexes[fileDesc].dataBlockNum, sizeof(int));

      // Set dirty and unpin metadata block
      BF_Block_SetDirty(block);
      CHK_BF_ERR(BF_UnpinBlock(block));
    }
    // Else call recursive function rec_trav_insert for the tree root
    else {
      RecTravOut possible_block = rec_trav_insert(fileDesc,
                                              OpenIndexes[fileDesc].rootBlockNum,
                                              value1,
                                              value2);
      // In case of an error inside the function
      if (possible_block.error < 0)
        return possible_block.error;
      // If the returned struct's possible_block.nblock_id is not -1, that
      // means that a new block has been created in the same level a the root,
      // so a new root should be created to point to both this and the new block
      if (possible_block.nblock_id != -1) {
        printf("KAPPAKIPPO\n" );
        // Allocate block for root
        CHK_BF_ERR(BF_AllocateBlock(fd, block));
        // Initialize root block with id and key_number (1)
        char* new_root_data = BF_Block_GetData(block);
        // Get new root block id
        int new_root_id = 0;
        CHK_BF_ERR(BF_GetBlockCounter(fd, &new_root_id));
        new_root_id--;
        // It is an "index block"
        char rid[4] = ".ib";
        memcpy(new_root_data, rid, 4);
        new_root_data += 4;
        // One key will be inserted
        int temp_i = 1;
        memcpy(new_root_data, &temp_i, sizeof(int));
        new_root_data += sizeof(int);
        // Block number before the first key is the previous root
        temp_i = OpenIndexes[fileDesc].rootBlockNum;
        memcpy(new_root_data, &temp_i, sizeof(int));
        new_root_data += sizeof(int);
        // Finally save key, then the new data block id in the new root
        memcpy(new_root_data, possible_block.nblock_strt_key,
               OpenIndexes[fileDesc].attrLength1);
        new_root_data += OpenIndexes[fileDesc].attrLength1;
        memcpy(new_root_data, &possible_block.nblock_id, sizeof(int));

        // After the data transfer from the possible_block struct, free the
        // allocated memory for saving the key
        free(possible_block.nblock_strt_key);

        // Set dirty and unpin new root block
        BF_Block_SetDirty(block);
        new_root_data = NULL;
        CHK_BF_ERR(BF_UnpinBlock(block));

        // Save the new root id in the global struct OpenIndexes
        OpenIndexes[fileDesc].rootBlockNum = new_root_id;

        // Change Metadata block
        CHK_BF_ERR(BF_GetBlock(fd, 0, block));
        char* meta_data = BF_Block_GetData(block);
        meta_data += 4 + 1 + sizeof(int) + 1 + sizeof(int);
        // Insert root block number
        memcpy(meta_data, &OpenIndexes[fileDesc].rootBlockNum, sizeof(int));

        // Set dirty and unpin metadata block
        BF_Block_SetDirty(block);
        CHK_BF_ERR(BF_UnpinBlock(block));
      }
    }
    free(curr_key);
  }
  BF_Block_Destroy(&block);
  return ret_value;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) { //fileDesc isthe location in OpenIndexes

  // Check if file is open in OpenIndexes[fileDesc]
  int fd = OpenIndexes[fileDesc].fd;
  int scanDesc;
  int i;
  if (fd == -1) {
    AM_errno = AME_INDEX_FILE_NOT_OPEN;
    return AME_INDEX_FILE_NOT_OPEN;
  }
  //Check if there is a root in the file
  if (OpenIndexes[fileDesc].rootBlockNum == -1) {
    AM_errno = AME_ROOT_NOT_EXIST;
    return AME_ROOT_NOT_EXIST;
  }

  for(scanDesc = 0; scanDesc < MAXSCANS; scanDesc++) {//search for location in OpenSearches
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
  int key_sz1=OpenIndexes[fileDesc].attrLength1;
  int key_sz2=OpenIndexes[fileDesc].attrLength2;
  int pointer_sz=sizeof(int);
  int key_num_sz=sizeof(int);
/////////////////////////////////////

  BF_Block *block;
  BF_Block_Init(&block);
  char* data;

  char* id=(char*)malloc(id_sz);    //id of the block
  int key_number;                   //how many numbers
  int ipointer;                     //the pointer for next indexblock
  char* key1=(char*)malloc(key_sz1);//key1 value
  char* key2=(char*)malloc(key_sz2);//key2 value
  int dpointer;                     //the pointer for the next data block

  int block_num;

  if(op==EQUAL || op==GREATER_THAN || op==GREATER_THAN_OR_EQUAL) { //we have to search the block

      block_num=OpenIndexes[fileDesc].rootBlockNum;
      while (1){
        CHK_BF_ERR(BF_GetBlock(fd,block_num,block));  //get block with number block_num
        data=BF_Block_GetData(block);                 //get the data
        memcpy(id,data,id_sz);
        if(v_cmp('c',id,".ib")==0){                   // indexblock
            memcpy(&key_number, data + id_sz , key_num_sz);
            memcpy(&ipointer,data + id_sz + key_num_sz , pointer_sz);

            for(int i=0;i<key_number;i++){    //check always if the value is smaller

                memcpy(key1,data + id_sz + key_num_sz + (i+1)*pointer_sz + i*key_sz1,key_sz1);

                if(v_cmp(OpenIndexes[fileDesc].attrType1,(void*)key1,value)<0){ //go to the left pointer

                    if(i==0 && ipointer==-1) {  //there is a chance that the first pointer is -1
                        AM_errno = AME_KEY_NOT_EXIST; //if thats the case the key does not exist in the tree
                        return AME_KEY_NOT_EXIST;
                    }

                    CHK_BF_ERR(BF_UnpinBlock(block)); //next pointer found so unpin the block
                    block_num=ipointer;
                    break;
                }

                memcpy(&ipointer,data + id_sz + key_num_sz + (i+1)*pointer_sz + (i+1)*key_sz1,pointer_sz);
            }//for finished

            CHK_BF_ERR(BF_UnpinBlock(block));//checked all the left pointers for each key so the last remains
            block_num=ipointer;
        }
        else{//datablock
            break;//we break from while loop
        }
      }
        memcpy(&key_number,data+id_sz,key_num_sz);//we passed the identification
        memcpy(&dpointer,data+id_sz+key_num_sz,pointer_sz);

        for(i=0;i<key_number;i++){ //find key1 inside the db block
          memcpy(key1,data + id_sz + key_num_sz + pointer_sz + i*key_sz1 +i*key_sz2,key_sz1);
          //no reason to make different if statements
          //we did only because one if statement would be too large
          if(v_cmp(OpenIndexes[fileDesc].attrType1,(void*)key1,value)==0 && op==EQUAL ){//found
              OpenSearches[scanDesc]=searchdata_add_info(OpenSearches[scanDesc],fileDesc,op,block_num,i,value);//add it to OpenSearches
              break;
          }
          else if(v_cmp(OpenIndexes[fileDesc].attrType1,(void*)key1,value)>0 && op==GREATER_THAN ){
              OpenSearches[scanDesc]=searchdata_add_info(OpenSearches[scanDesc],fileDesc,op,block_num,i,value);//add it to OpenSearches
              break;
          }
          else if(v_cmp(OpenIndexes[fileDesc].attrType1,(void*)key1,value)>=0 && op==GREATER_THAN_OR_EQUAL ){
              OpenSearches[scanDesc]=searchdata_add_info(OpenSearches[scanDesc],fileDesc,op,block_num,i,value);//add it to OpenSearches
              break;
          }
        }
        if(i==key_number){//for finished without finding anything so key does not exist
            AM_errno = AME_KEY_NOT_EXIST;
            return AME_KEY_NOT_EXIST;
         }

  }
  else if(op==NOT_EQUAL || op==LESS_THAN || op==LESS_THAN_OR_EQUAL){ //we choose the left most block

      block_num=OpenIndexes[fileDesc].dataBlockNum;
      OpenSearches[scanDesc]=searchdata_add_info(OpenSearches[scanDesc],fileDesc,op,block_num,0,value);//add it to OpenSearches

  }
  else{
      free(id);
      free(key1);
      free(key2);
      AM_errno = AME_INVALID_OP;
      return AME_INVALID_OP;
  }

  free(id);
  free(key1);
  free(key2);
  BF_Block_Destroy(&block);

  return scanDesc;
}


void *AM_FindNextEntry(int scanDesc) { //loaction in searchdata
    int id_sz=4;
    int fileDesc=OpenSearches[scanDesc].fileDesc;
    int key_sz1=OpenIndexes[fileDesc].attrLength1;
    int key_sz2=OpenIndexes[fileDesc].attrLength2;
    int pointer_sz=sizeof(int);
    int key_num_sz=sizeof(int);


    BF_Block *block;
    BF_Block_Init(&block);
    char* data;
    int pointer;
    char* key1=(char*)malloc(key_sz1);//key1 value
    char* key2=(char*)malloc(key_sz2);//key2 value

    int fd = OpenIndexes[fileDesc].fd;
    int curr_block=OpenSearches[scanDesc].curr_block;
    int curr_pos=OpenSearches[scanDesc].curr_pos;
    int op=OpenSearches[scanDesc].op;
    void* op_key=OpenSearches[scanDesc].op_key;
    int key_num;
    CHK_BF_ERR_NULL(BF_GetBlock(fd,curr_block,block));  //get block with number block_num
    data=BF_Block_GetData(block);

    memcpy(&pointer,data + id_sz + key_num_sz , pointer_sz);
    memcpy(&key_num,data + id_sz  , key_num_sz);

    if(pointer==-1 && key_num==curr_pos) {//there is no next entry
        AM_errno = AME_END_OF_FILE;
        return NULL;
    }

    if(key_num==curr_pos) {//go to the next block
            CHK_BF_ERR_NULL(BF_UnpinBlock(block));
            OpenSearches[scanDesc].curr_pos=0;          //new block so our position is 0
            OpenSearches[scanDesc].curr_block=pointer;  //our curr_block now is the pointer of the prwvious block
            CHK_BF_ERR_NULL(BF_GetBlock(fd,pointer,block)); //get new block
            data=BF_Block_GetData(block);
    }

    memcpy(key1,data + id_sz + key_num_sz + pointer_sz + curr_pos*key_sz1 + curr_pos*key_sz2 ,key_sz1 );
    //memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
    switch(op) {
        case EQUAL :
            if(v_cmp(OpenIndexes[fileDesc].attrType1,key1,op_key)==0){
                memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
                OpenSearches[scanDesc].curr_pos++;
            }
            else{
                AM_errno = AME_EOF;
                return NULL;
            }
            break;
        case NOT_EQUAL :
            if(v_cmp(OpenIndexes[fileDesc].attrType1,key1,op_key)!=0) {
                memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
                OpenSearches[scanDesc].curr_pos++;
            }
            else if(v_cmp(OpenIndexes[fileDesc].attrType1,key1,op_key)==0) {
                while(1){ //if key1 is equal to op_key we search for a different key1
                    OpenSearches[scanDesc].curr_pos++;
                    if(key_num==curr_pos) {//go to the next block
                        CHK_BF_ERR_NULL(BF_UnpinBlock(block));
                        OpenSearches[scanDesc].curr_pos=0;          //new block so our position is 0
                        OpenSearches[scanDesc].curr_block=pointer;  //our curr_block now is the pointer of the prwvious block
                        CHK_BF_ERR_NULL(BF_GetBlock(fd,pointer,block)); //get new block
                        data=BF_Block_GetData(block);
                        memcpy(&pointer,data + id_sz + key_num_sz , pointer_sz); //pointer of new block
                    }
                    memcpy(key1,data + id_sz + key_num_sz + pointer_sz + curr_pos*key_sz1 + curr_pos*key_sz2 ,key_sz1 );
                    if(v_cmp(OpenIndexes[fileDesc].attrType1,key1,op_key)==0)
                        continue;
                    else {
                        memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
                        OpenSearches[scanDesc].curr_pos++;
                        break;
                    }
                }
            }

            break;

        case LESS_THAN :
            if(v_cmp(OpenIndexes[fileDesc].attrType1,key1,op_key)>=0) {  //stop if GREATER_THAN_OR_EQUAL
                AM_errno = AME_EOF;
                return NULL;
            }
            else{
                memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
                OpenSearches[scanDesc].curr_pos++;
            }
            break;

        case GREATER_THAN :
            memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
            OpenSearches[scanDesc].curr_pos++;
            break;

        case LESS_THAN_OR_EQUAL :
            if(v_cmp(OpenIndexes[fileDesc].attrType1,key1,op_key)>0) {   //stop if GREATER_THAN
                AM_errno = AME_EOF;
                return NULL;
            }
            else{
                memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
                OpenSearches[scanDesc].curr_pos++;
            }
            break;

        case GREATER_THAN_OR_EQUAL :
            memcpy(key2,data + id_sz + key_num_sz + pointer_sz + (curr_pos+1)*key_sz1 + curr_pos*key_sz2 ,key_sz2 );
            OpenSearches[scanDesc].curr_pos++;
            break;
    }


    OpenSearches[scanDesc].info=(void*)malloc(OpenIndexes[OpenSearches[scanDesc].fileDesc].attrLength2);
    OpenSearches[scanDesc].info=(void*)key2;
    free(key1);
    BF_Block_Destroy(&block);

    return OpenSearches[scanDesc].info;
}


int AM_CloseIndexScan(int scanDesc) {

  if(OpenSearches[scanDesc].fileDesc==-1) { //check if in OpenIndexes
    AM_errno = AME_CANNOT_DESTROY_SEARCH_OPEN;
    return AME_CANNOT_DESTROY_SEARCH_OPEN;
  }
  else //if it is free it
    searchdata_free(&OpenSearches[scanDesc]);
  return AME_OK;
}


void AM_PrintError(char *errString) {
  printf("errString = %s\n\n",errString);
  switch(AM_errno) {
      case AME_OK : printf("AME_OK\n"); break;
      case AME_EOF : printf("AME_EOF\n"); break;
      case AME_BF_CALL_ERROR : printf("AME_BF_CALL_ERROR\n"); break;
      case AME_CREATE_INPUT_ERROR : printf("AME_CREATE_INPUT_ERROR\n"); break;
      case AME_NO_SPACE_FOR_INDEX : printf("AME_NO_SPACE_FOR_INDEX\n"); break;
      case AME_NOT_INDEX_FILE : printf("AME_NOT_INDEX_FILE\n"); break;
      case AME_CANNOT_DESTROY_INDEX_OPEN : printf("AME_CANNOT_DESTROY_INDEX_OPEN\n"); break;
      case AME_INDEX_FILE_NOT_OPEN : printf("AME_INDEX_FILE_NOT_OPEN\n"); break;
      case AME_ROOT_NOT_EXIST : printf("AME_ROOT_NOT_EXIST\n"); break;
      case AME_INVALID_OP : printf("AME_INVALID_OP\n"); break;
      case AME_NO_SPACE_FOR_SEARCH : printf("AME_NO_SPACE_FOR_SEARCH\n"); break;
      case AME_CANNOT_DESTROY_SEARCH_OPEN : printf("AME_CANNOT_DESTROY_SEARCH_OPEN\n"); break;
      case AME_KEY_NOT_EXIST : printf("AME_KEY_NOT_EXIST\n"); break;
      case AME_END_OF_FILE : printf("AME_END_OF_FILE\n"); break;
    }
}

void AM_Close() {
  //CLOSE KAI TI ALLO DEN KSERW
  BF_Close();
}
