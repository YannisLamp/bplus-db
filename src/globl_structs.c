#include <stdio.h>
#include "globl_structs.h"

FileMeta filemeta_init(FileMeta fm) {
  // Initilize input FileMeta struct
  fm.fileDesc = -1;
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
    sd.starting_block=-1;
    sd.starting_pos=-1;
    sd.curr_block=-1;
    sd.curr_pos=-1;

    return sd;
}
