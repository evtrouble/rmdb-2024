/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "executor_explain.h"
#include "index/ix.h"
#include "record_printer.h"

const char *help_info = "Supported SQL syntax:\n"
                        "  command ;\n"
                        "command:\n"
                        "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                        "  DROP TABLE table_name\n"
                        "  CREATE INDEX table_name (column_name)\n"
                        "  DROP INDEX table_name (column_name)\n"
                        "  INSERT INTO table_name VALUES (value [, value ...])\n"
                        "  DELETE FROM table_name [WHERE where_clause]\n"
                        "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                        "  SELECT selector FROM table_name [WHERE where_clause]\n"
                        "type:\n"
                        "  {INT | FLOAT | CHAR(n)}\n"
                        "where_clause:\n"
                        "  condition [AND condition ...]\n"
                        "condition:\n"
                        "  column op {column | value}\n"
                        "column:\n"
                        "  [table_name.]column_name\n"
                        "op:\n"
                        "  {= | <> | < | > | <= | >=}\n"
                        "selector:\n"
                        "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context)
{
    if (auto x = std::dynamic_pointer_cast<ExplainPlan>(plan))
    {
        // 创建ExplainExecutor来处理EXPLAIN语句
        ExplainExecutor executor(x);
        executor.init();

        // 获取执行计划的字符串表示
        std::string result = executor.get_result();

        // 将结果写入context
        memcpy(context->data_send_ + *(context->offset_), result.c_str(), result.length());
        *(context->offset_) += result.length();

        // 将结果写入output.txt文件
        int fd = ::open("output.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd != -1)
        {
            std::string buffer = result;
            [[maybe_unused]] ssize_t discard = ::write(fd, buffer.data(), buffer.size());
            close(fd);
        }
    }
    else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan))
    {
        switch (x->tag)
        {
        case T_CreateTable:
        {
            sm_manager_->create_table(x->tab_name_, x->cols_, context);
            break;
        }
        case T_DropTable:
        {
            sm_manager_->drop_table(x->tab_name_, context);
            break;
        }
        case T_CreateIndex:
        {
            sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
            break;
        }
        case T_DropIndex:
        {
            sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
            break;
        }
        case T_ShowIndex:
        {
            sm_manager_->show_index(x->tab_name_, context);
            break;
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context)
{
    switch (plan->tag)
    {
    case T_Help:
    {
        int help_len = strlen(help_info);
        if (help_len + 1 > MAX_BUFFER_SIZE - *(context->offset_))
        { // 检查缓冲区大小
            throw InternalError("Buffer overflow when sending help info");
        }
        memcpy(context->data_send_ + *(context->offset_), help_info, help_len);
        *(context->offset_) += help_len;
        context->data_send_[*(context->offset_)] = '\0'; // 确保字符串以null结尾
        break;
    }
    case T_ShowTable:
    {
        sm_manager_->show_tables(context);
        break;
    }
    case T_DescTable:
    {
        auto x = std::static_pointer_cast<OtherPlan>(plan);
        sm_manager_->desc_table(x->tab_name_, context);
        break;
    }
    case T_Transaction_begin:
    {
        // 显示开启一个事务
        context->txn_->set_txn_mode(true);
        break;
    }
    case T_Transaction_commit:
    {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->commit(context, context->log_mgr_);
        break;
    }
    case T_Transaction_rollback:
    {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->abort(context, context->log_mgr_);
        break;
    }
    case T_Transaction_abort:
    {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->abort(context, context->log_mgr_);
        break;
    }
    case T_Create_StaticCheckPoint:
    {
        context->log_mgr_->create_static_check_point();
        break;
    }
    case T_SetKnob:
    {
        auto x = std::static_pointer_cast<SetKnobPlan>(plan);
        switch (x->set_knob_type_)
        {
        case ast::SetKnobType::EnableNestLoop:
        {
            planner_->set_enable_nestedloop_join(x->bool_val_);
            break;
        }
        case ast::SetKnobType::EnableSortMerge:
        {
            planner_->set_enable_sortmerge_join(x->bool_val_);
            break;
        }
        default:
        {
            throw RMDBError("Not implemented!\n");
            break;
        }
        }
        break;
    }
    default:
        throw InternalError("Unexpected field type");
        break;
    }
}
std::string makeAggFuncCaptions(const TabCol &sel_col)
{
    switch (sel_col.aggFuncType)
    {
    case ast::AggFuncType::COUNT:
        return "COUNT(" + sel_col.col_name + ")";
    case ast::AggFuncType::SUM:
        return "SUM(" + sel_col.col_name + ")";
    case ast::AggFuncType::MAX:
        return "MAX(" + sel_col.col_name + ")";
    case ast::AggFuncType::MIN:
        return "MIN(" + sel_col.col_name + ")";
    case ast::AggFuncType::AVG:
        return "AVG(" + sel_col.col_name + ")";
    default:
        throw RMDBError();
    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, const std::vector<TabCol> &sel_cols,
                            Context *context)
{
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols)
    {
        if (sel_col.alias.empty())
        {
            if (ast::AggFuncType::NO_TYPE == sel_col.aggFuncType)
                captions.emplace_back(std::move(sel_col.col_name));
            else
                captions.emplace_back(std::move(makeAggFuncCaptions(sel_col)));
        }
        else
        {
            captions.emplace_back(sel_col.alias);
        }
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);

    int fd = ::open("output.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd == -1)
        return;

    // 64KB缓冲区
    std::string buffer;
    buffer.reserve(8096);

    buffer.append("|");
    for (size_t i = 0; i < captions.size(); ++i)
    {
        buffer.append(" ").append(captions[i]).append(" |");
    }
    buffer.append("\n");
    // Print records
    size_t num_rec = 0;
    [[maybe_unused]] ssize_t discard;

    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple())
    {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols())
        {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT)
            {
                auto val = *(int *)rec_buf;
                if (std::numeric_limits<int>::max() == val || std::numeric_limits<int>::min() == val)
                    col_str = "";
                else
                    col_str = std::to_string(*(int *)rec_buf);
            }
            else if (col.type == TYPE_FLOAT)
            {
                auto val = *(float *)rec_buf;
                if (std::numeric_limits<float>::max() == val || std::numeric_limits<float>::lowest() == val)
                    col_str = "";
                else {
                    std::stringstream ss;
                    ss << std::fixed << std::setprecision(FLOAT_PRECISION) << val;
                    col_str = ss.str();
                }
            }
            else if (col.type == TYPE_STRING)
            {
                col_str.reserve(col.len);
                for (int i = 0; i < col.len && rec_buf[i] != '\0'; i++)
                    col_str.append(1, rec_buf[i]);
            }
            columns.emplace_back(std::move(col_str));
        }
        // print record into buffer
        rec_printer.print_record(columns, context);
        // print record into file
        buffer.append("|");
        for (size_t i = 0; i < columns.size(); ++i)
        {
            buffer.append(" ").append(columns[i]).append(" |");
        }
        buffer.append("\n");
        num_rec++;
        // 缓冲区满时写入
        if (buffer.size() >= 8096)
        {
            discard = ::write(fd, buffer.data(), buffer.size());
            buffer.clear();
        }
    }
    // 写入剩余数据
    if (buffer.size())
    {
        discard = ::write(fd, buffer.data(), buffer.size());
    }
    close(fd);
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec)
{
    exec->Next();
}
// std::unordered_set<Value>
// QlManager::sub_select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, bool converse_to_float)
// {
//     std::unordered_set<Value> results;
//     // 确保一列
//     if (executorTreeRoot->cols().size() != 1)
//         throw RMDBError("subquery must have only one column");
//     const auto &col = executorTreeRoot->cols()[0];

//     // 执行query_plan
//     for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple())
//     {
//         auto Tuple = executorTreeRoot->Next();
//         char *rec_buf = Tuple->data + col.offset;
//         Value value;
//         switch (col.type)
//         {
//         case TYPE_INT:
//         {
//             auto val = *reinterpret_cast<int *>(rec_buf);
//             if (val != INT_MAX)
//             {
//                 if (!converse_to_float)
//                     value.set_int(val);
//                 else
//                     value.set_float(static_cast<float>(val));
//             }
//             break;
//         }
//         case TYPE_FLOAT:
//         {
//             auto val = *reinterpret_cast<float *>(rec_buf);
//             if (val != FLT_MAX)
//             {
//                 value.set_float(val);
//             }
//             break;
//         }
//         case TYPE_STRING:
//         {
//             std::string col_str(reinterpret_cast<char *>(rec_buf), col.len);
//             col_str.resize(strlen(col_str.c_str()));
//             value.set_str(col_str);
//             break;
//         }
//         default:
//         {
//             throw RMDBError("Unsupported column type");
//         }
//         }
//         results.insert(value);
//     }

//     return results;
// }