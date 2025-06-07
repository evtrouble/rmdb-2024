#include "transaction.h"
#include "transaction_manager.h"

Transaction::~Transaction() { txn_manager_->remove_txn(txn_id_); }