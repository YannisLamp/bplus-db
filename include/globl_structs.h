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


































#endif /* GLOBL_STRUCTS */
