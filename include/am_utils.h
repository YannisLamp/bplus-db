#ifndef AM_UTILS
#define AM_UTILS

int v_cmp(char, void*, void*);//kalutera na to kanoume char* afoy pernoume memcpy me char*

typedef struct RecTravOut {
    int nblock_id;
    void* nblock_strt_key;
    int error;
} RecTravOut;

int create_leftmost_block(int fileDesc, void *value1, void *value2);
int init_bptree(int fileDesc, void *value1, void *value2);
RecTravOut rec_trav_insert();


























#endif /* AM_UTILS */
