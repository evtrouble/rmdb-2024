#pragma once
#include <vector>
#include <cstring>
#include "defs.h"
#include "errors.h"

inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, col_len);
        default:
            throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}