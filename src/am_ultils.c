#include <string.h>
#include "defn.h"

// Compares variables depending on their type (input v_type)
// Output similar to strcmp (only with -2 output for input errors)
int v_cmp(char v_type, void* value1, void* value2) {
  if (v_type == INTEGER || v_type == FLOAT) {
    if (*value1 < *value2)
      return -1;
    else if (*value1 > *value2)
      return 1;
    else
      return 0;
  }
  else if (v_type == STRING)
    return strcmp(value1, value2);
  // In case of wrong v_type input
  else
    return -2;
}

int get_prev_dblock(fileDesc, key) {
  
}
