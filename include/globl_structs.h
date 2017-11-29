#ifndef GLOBL_STRUCTS
#define GLOBL_STRUCTS

typedef struct FileMeta {
    int fileDesc;
    char* fileName;
    char attrType1;
    int attrLength1;
    char attrType2;
    int attrLength2;
    int rootBlockNum;
    int dataBlockNum;
} FileMeta;

typedef struct SearchData {
    int fileDesc;
    int op;
    int key_in_block;
    int key_pos;
    int curr_block;
    int curr_pos;
} SearchData;


































#endif /* GLOBL_STRUCTS */