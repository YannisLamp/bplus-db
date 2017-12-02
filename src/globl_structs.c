#include <stdio.h>
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
    sd.op_key=-1;
    sd.info=NULL;
    return sd;
}

SearchData searchdata_add_info(SearchData sd,int fileDesc,int op,int block,int pos,int op_key){
  sd.fileDesc=fileDesc;
  sd.op=op;
  sd.curr_block=block;
  sd.curr_pos=pos;
  sd.op_key=op_key;
  sd.info=NULL;
  return sd;
}


void searchdata_free(OpenSearches* sd) {
  sd->fileDesc=-1;
  sd->op=-1;
  sd->curr_block=-1;
  sd->curr_pos=-1;
  sd->op_key=-1;
  free(sd->info);
  sd.info=NULL;
}
