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
    void * info;
    int fileDesc;//fileDesc is the location in OpenIndexes
    int op;
    int curr_block;   //block we are in
    int curr_pos;     //position
    void* op_key; //different meaning deppending on op


} SearchData;

SearchData searchdata_init(SearchData);
SearchData searchdata_add_info(SearchData,int,int,int,int,int); //thats the function we use
                                                      //at AM_OpenIndexScan
                                                      //starting and curr are the same




void searchdata_free(SearchData*);                   //free the info and initializethe data













#endif /* GLOBL_STRUCTS */
