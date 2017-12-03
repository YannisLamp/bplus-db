#include <stdio.h>
#include <stdlib.h>
#include "globl_structs.h"

FileMeta filemeta_init(FileMeta fm) {
  // Initilize input FileMeta struct
  fm.fd = -1;
  fm.fileName = NULL;
  fm.attrType1 = '\0';
  fm.attrLength1 = -1;
  fm.attrType2 = '\0';
  fm.attrLength2 = -1;
  fm.rootBlockNum = -1;
  fm.dataBlockNum = -1;

  return fm;
}

SearchData searchdata_init(SearchData sd) {
    sd.fileDesc=-1;
    sd.op=-1;
    sd.curr_block=-1;
    sd.curr_pos=-1;
    sd.op_key=NULL;
    sd.info=NULL;
    return sd;
}

SearchData searchdata_add_info(SearchData sd,int fileDesc,int op,int block,int pos,void* op_key){
  sd.fileDesc=fileDesc;
  sd.op=op;
  sd.curr_block=block;
  sd.curr_pos=pos;
  sd.op_key=op_key;
  sd.info=NULL;
  return sd;
}


void searchdata_free(SearchData* sd) {
  sd->fileDesc=-1;
  sd->op=-1;
  sd->curr_block=-1;
  sd->curr_pos=-1;
  sd->op_key=NULL;
  free(sd->info);
  sd->info=NULL;
}
