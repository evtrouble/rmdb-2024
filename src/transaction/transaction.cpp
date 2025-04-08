#include "transaction.h"
// 事务使用一些特殊的字段，放到每行记录中，表示行记录的可见性。
std::vector<ColMeta> Transaction::fields_{
    // field_id in trx fields is invisible.
    ColMeta{.name = "__trx_xid_begin", .type = ColType::TYPE_INT, 4 /*attr_len*/, 0 /*attr_offset*/, false},
    ColMeta{.name = "__trx_xid_end", .type = ColType::TYPE_INT, 4 /*attr_len*/, 4 /*attr_offset*/, false}
  };