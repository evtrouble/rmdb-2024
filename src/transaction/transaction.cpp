#include "transaction.h"
#include "transaction_manager.h"

void Transaction::release() {
    if(commit_ts_ == 0) [[unlikely]] return;
    if(ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        txn_manager_->remove_txn(txn_id_);
        delete this;
    }
}