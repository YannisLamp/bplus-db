#ifndef AM_UTILS
#define AM_UTILS

int v_cmp(char, void*, void*);

typedef struct RecTravOut {
    int nblock_id;
    void* nblock_strt_key;
} RecTravOut;

RecTravOut rec_trav_insert();


























#endif /* AM_UTILS */
