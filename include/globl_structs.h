#ifndef GLOBL_STRUCTS
#define GLOBL_STRUCTS

typedef struct FileMeta {
    int fd;
    char* fileName;
    char attrType1;
    int attrLength1;
    char attrType2;
    int attrLength2;
    int rootBlockNum;
    int dataBlockNum;
} FileMeta;

FileMeta filemeta_init(FileMeta);

typedef struct SearchData {
    int fileDesc;
    int op;
    int starting_block;
    int starting_pos;
    int curr_block;
    int curr_pos;
} SearchData;

SearchData searchdata_init(SearchData )

































#endif /* GLOBL_STRUCTS */
