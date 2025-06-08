#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "optimizer/plan.h"
#include "parser/parser.h"
#include "record/rm.h"
#include "common/common.h"
#include <sstream>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <iostream>

class ExplainExecutor : public AbstractExecutor
{
private:
    std::shared_ptr<Plan> plan_;
    std::string result_;
    bool done_;

    // 节点类型优先级枚举
    enum class NodePriority
    {
        FILTER = 1,
        JOIN = 2,
        PROJECT = 3,
        SCAN = 4,
        OTHER = 5
    };

    // 获取节点类型的优先级
    NodePriority getNodePriority(PlanTag tag)
    {
        switch (tag)
        {
        case T_Filter:
            return NodePriority::FILTER;
        case T_NestLoop:
        case T_SortMerge:
            return NodePriority::JOIN;
        case T_Projection:
            return NodePriority::PROJECT;
        case T_SeqScan:
        case T_IndexScan:
            return NodePriority::SCAN;
        default:
            return NodePriority::OTHER;
        }
    }

    // 子节点信息结构
    struct ChildNodeInfo
    {
        std::shared_ptr<Plan> plan;
        NodePriority priority;
        std::string output;

        ChildNodeInfo(std::shared_ptr<Plan> p, NodePriority prio, const std::string &out)
            : plan(p), priority(prio), output(out) {}
    };

    // 获取表的实际显示名称（别名或原名）
    std::string get_table_display_name(const std::string &original_name, const std::shared_ptr<ast::SelectStmt> &stmt)
    {
        if (!stmt)
            return original_name;
        for (size_t i = 0; i < stmt->tabs.size(); i++)
        {
            if (stmt->tabs[i] == original_name && i < stmt->tab_aliases.size() && !stmt->tab_aliases[i].empty())
            {
                return stmt->tab_aliases[i];
            }
        }
        return original_name;
    }

    // 递归生成查询计划树的字符串表示（新版本支持子节点排序）
    std::string explain_plan(const std::shared_ptr<Plan> &plan, int depth = 0, std::shared_ptr<ast::SelectStmt> stmt = nullptr)
    {
        if (!plan)
            return "";

        std::string result;
        std::string indent_str(depth, '\t');

        switch (plan->tag)
        {
        case T_select:
        {
            auto select_plan = std::static_pointer_cast<DMLPlan>(plan);
            if (select_plan->subplan_)
            {
                result += explain_plan(select_plan->subplan_, depth, stmt);
            }
            break;
        }
        case T_Projection:
        {
            auto proj_plan = std::static_pointer_cast<ProjectionPlan>(plan);

            // 处理当前节点
            std::string proj_str = "Project(columns=";

            // 检查是否是 SELECT * 查询
            if (stmt && stmt->is_select_star_query())
            {
                // 如果是 SELECT *，直接输出 [*]
                proj_str += "[*])\n";
            }
            else
            {
                std::vector<std::string> columns;
                for (const auto &col : proj_plan->sel_cols_)
                {
                    if (col.col_name == "*")
                    {
                        // 如果遇到单独的 *，也输出 [*]
                        proj_str += "[*])\n";
                        break;
                    }
                    else
                    {
                        std::string tab_name = col.tab_name;
                        if (stmt)
                        {
                            tab_name = stmt->find_alias(tab_name);
                        }
                        columns.push_back(tab_name + "." + col.col_name);
                    }
                }
                if (!columns.empty())
                {
                    std::sort(columns.begin(), columns.end());
                    proj_str += "[" + format_list(columns) + "])\n";
                }
            }

            result += indent_str + proj_str;

            // 处理子计划
            if (proj_plan->subplan_)
            {
                result += explain_plan(proj_plan->subplan_, depth + 1, stmt);
            }
            break;
        }
        case T_SeqScan:
        case T_IndexScan:
        {
            auto scan_plan = std::static_pointer_cast<ScanPlan>(plan);
            std::string tab_name = scan_plan->tab_name_;

            // 如果有过滤条件，先添加Filter节点
            if (!scan_plan->fed_conds_.empty())
            {
                std::vector<std::string> conditions;
                for (const auto &cond : scan_plan->fed_conds_)
                {
                    if (cond.lhs_col.tab_name != scan_plan->tab_name_)
                        continue;

                    std::string lhs_tab_name = cond.lhs_col.tab_name;
                    if (stmt)
                    {
                        lhs_tab_name = stmt->find_alias(lhs_tab_name);
                    }
                    std::string condition = lhs_tab_name + "." + cond.lhs_col.col_name;
                    condition += get_op_string(cond.op);
                    if (cond.is_rhs_val)
                    {
                        condition += get_value_string(cond.rhs_val);
                    }
                    else
                    {
                        std::string rhs_tab_name = cond.rhs_col.tab_name;
                        if (stmt)
                        {
                            rhs_tab_name = stmt->find_alias(rhs_tab_name);
                        }
                        condition += rhs_tab_name + "." + cond.rhs_col.col_name;
                    }
                    conditions.push_back(condition);
                }
                if (!conditions.empty())
                {
                    std::sort(conditions.begin(), conditions.end());
                    result += indent_str + "Filter(condition=[" + format_list(conditions) + "])\n";
                    result += std::string(depth + 1, '\t') + (plan->tag == T_SeqScan ? "Scan" : "IndexScan") + std::string("(table=") + tab_name + ")\n";
                }
                else
                {
                    result += indent_str + (plan->tag == T_SeqScan ? "Scan" : "IndexScan") + std::string("(table=") + tab_name + ")\n";
                }
            }
            else
            {
                result += indent_str + (plan->tag == T_SeqScan ? "Scan" : "IndexScan") + std::string("(table=") + tab_name + ")\n";
            }
            break;
        }
        case T_NestLoop:
        case T_SortMerge:
        {
            auto join_plan = std::static_pointer_cast<JoinPlan>(plan);
            std::vector<std::string> conditions;
            std::set<std::string> table_set;

            collect_tables(join_plan, table_set);
            std::vector<std::string> tables(table_set.begin(), table_set.end());
            std::sort(tables.begin(), tables.end());

            for (const auto &cond : join_plan->conds_)
            {
                if (!cond.is_rhs_val)
                {
                    std::string lhs_tab_name = cond.lhs_col.tab_name;
                    std::string rhs_tab_name = cond.rhs_col.tab_name;
                    if (stmt)
                    {
                        lhs_tab_name = stmt->find_alias(lhs_tab_name);
                        rhs_tab_name = stmt->find_alias(rhs_tab_name);
                    }
                    std::string condition = lhs_tab_name + "." + cond.lhs_col.col_name + "=" +
                                            rhs_tab_name + "." + cond.rhs_col.col_name;
                    conditions.push_back(condition);
                }
            }
            std::sort(conditions.begin(), conditions.end());

            result += indent_str + "Join(tables=[" + format_list(tables) + "]" +
                      (conditions.empty() ? "" : ",condition=[" + format_list(conditions) + "]") +
                      ")\n";

            // **关键修改：收集所有子节点并按优先级排序**
            std::vector<ChildNodeInfo> child_nodes;

            if (join_plan->left_)
            {
                std::string left_output = explain_plan(join_plan->left_, depth + 1, stmt);
                child_nodes.emplace_back(join_plan->left_, getNodePriority(join_plan->left_->tag), left_output);
            }
            if (join_plan->right_)
            {
                std::string right_output = explain_plan(join_plan->right_, depth + 1, stmt);
                child_nodes.emplace_back(join_plan->right_, getNodePriority(join_plan->right_->tag), right_output);
            }

            // 按优先级排序子节点（Filter < Join < Project < Scan）
            std::sort(child_nodes.begin(), child_nodes.end(),
                      [](const ChildNodeInfo &a, const ChildNodeInfo &b)
                      {
                          return a.priority < b.priority;
                      });

            // 按排序后的顺序输出子节点
            for (const auto &child : child_nodes)
            {
                result += child.output;
            }
            break;
        }
        case T_Filter:
        {
            auto filter_plan = std::static_pointer_cast<FilterPlan>(plan);
            std::vector<std::string> conditions;

            for (const auto &cond : filter_plan->conds_)
            {
                std::string lhs_tab_name = cond.lhs_col.tab_name;
                if (stmt)
                {
                    lhs_tab_name = stmt->find_alias(lhs_tab_name);
                }
                std::string condition = lhs_tab_name + "." + cond.lhs_col.col_name;
                condition += get_op_string(cond.op);
                if (cond.is_rhs_val)
                {
                    condition += get_value_string(cond.rhs_val);
                }
                else
                {
                    std::string rhs_tab_name = cond.rhs_col.tab_name;
                    if (stmt)
                    {
                        rhs_tab_name = stmt->find_alias(rhs_tab_name);
                    }
                    condition += rhs_tab_name + "." + cond.rhs_col.col_name;
                }
                conditions.push_back(condition);
            }

            std::sort(conditions.begin(), conditions.end());
            result += indent_str + "Filter(condition=[" + format_list(conditions) + "])\n";

            if (filter_plan->subplan_)
            {
                result += explain_plan(filter_plan->subplan_, depth + 1, stmt);
            }
            break;
        }
        default:
            result = indent_str + "Unknown\n";
            break;
        }

        return result;
    }

    // 辅助函数：获取操作符字符串表示
    std::string get_op_string(CompOp op)
    {
        switch (op)
        {
        case OP_EQ:
            return "=";
        case OP_NE:
            return "<>";
        case OP_LT:
            return "<";
        case OP_GT:
            return ">";
        case OP_LE:
            return "<=";
        case OP_GE:
            return ">=";
        default:
            return "unknown";
        }
    }

    // 辅助函数：获取值的字符串表示
    std::string get_value_string(const Value &val)
    {
        std::stringstream ss;
        switch (val.type)
        {
        case TYPE_INT:
            ss << val.int_val;
            break;
        case TYPE_FLOAT:
            ss << val.float_val;
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            ss << "'" << val.str_val << "'";
            break;
        default:
            ss << "unknown_type";
            break;
        }
        return ss.str();
    }

    // 辅助函数：收集所有表名
    void collect_tables(const std::shared_ptr<Plan> &plan, std::set<std::string> &table_set)
    {
        if (!plan)
            return;

        switch (plan->tag)
        {
        case T_SeqScan:
        case T_IndexScan:
        {
            auto scan_plan = std::static_pointer_cast<ScanPlan>(plan);
            table_set.insert(scan_plan->tab_name_);
            break;
        }
        case T_Projection:
        {
            auto proj_plan = std::static_pointer_cast<ProjectionPlan>(plan);
            collect_tables(proj_plan->subplan_, table_set);
            break;
        }
        case T_NestLoop:
        case T_SortMerge:
        {
            auto join_plan = std::static_pointer_cast<JoinPlan>(plan);
            collect_tables(join_plan->left_, table_set);
            collect_tables(join_plan->right_, table_set);
            break;
        }
        case T_Filter:
        {
            auto filter_plan = std::static_pointer_cast<FilterPlan>(plan);
            collect_tables(filter_plan->subplan_, table_set);
            break;
        }
        default:
            break;
        }
    }

    // 辅助函数：检查计划树是否包含过滤条件
    bool has_filter_conditions(const std::shared_ptr<Plan> &plan)
    {
        if (!plan)
            return false;

        // 检查扫描节点的过滤条件
        if (auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan))
        {
            return !scan_plan->fed_conds_.empty();
        }

        // 检查连接节点的子树
        if (auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan))
        {
            return has_filter_conditions(join_plan->left_) ||
                   has_filter_conditions(join_plan->right_);
        }

        return false;
    }

public:
    ExplainExecutor(std::shared_ptr<Plan> plan)
        : plan_(std::move(plan)), done_(false) {}

    void init()
    {
        if (auto explain_plan = std::dynamic_pointer_cast<ExplainPlan>(plan_))
        {
            if (explain_plan->subplan_)
            {
                // 获取SelectStmt
                std::shared_ptr<ast::SelectStmt> select_stmt = nullptr;
                if (auto dml_plan = std::dynamic_pointer_cast<DMLPlan>(explain_plan->subplan_))
                {
                    // 获取原始的SelectStmt
                    if (auto select_node = std::dynamic_pointer_cast<ast::SelectStmt>(dml_plan->parse))
                    {
                        select_stmt = select_node;
                    }
                }
                result_ = this->explain_plan(explain_plan->subplan_, 0, select_stmt);
            }
            else
            {
                result_ = "Empty plan\n";
            }
        }
        else
        {
            result_ = "Invalid plan type\n";
        }
    }

    bool next()
    {
        if (done_)
            return false;
        done_ = true;
        return true;
    }

    std::string get_result() const
    {
        return result_;
    }

    std::string getType() override { return "ExplainExecutor"; }

    ExecutionType type() const override { return ExecutionType::EXPLAIN; }

    bool is_end() const override { return done_; }

    Rid &rid() override { return _abstract_rid; }

    std::string plan_to_string(std::shared_ptr<Plan> plan, int indent = 0)
    {
        std::string result;
        std::string indent_str(indent, '\t'); // 使用\t作为缩进

        switch (plan->tag)
        {
        case T_Projection:
        {
            auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            std::vector<std::string> cols;

            // 检查是否是 SELECT *
            bool is_select_all = proj_plan->sel_cols_.empty();
            if (is_select_all)
            {
                result = indent_str + "Project(columns=[*])\n";
            }
            else
            {
                for (const auto &col : proj_plan->sel_cols_)
                {
                    cols.push_back(format_tabcol(col));
                }
                result = indent_str + "Project(columns=" + format_list(cols) + ")\n";
            }
            result += plan_to_string(proj_plan->subplan_, indent + 1);
            break;
        }
        case T_SeqScan:
        {
            auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
            if (!scan_plan->fed_conds_.empty())
            {
                std::vector<std::string> conditions;
                for (const auto &cond : scan_plan->fed_conds_)
                {
                    conditions.push_back(format_condition(cond));
                }
                result = indent_str + "Filter(condition=" + format_list(conditions) + ")\n";
                result += indent_str + "\tScan(table=" + scan_plan->tab_name_ + ")\n";
            }
            else
            {
                result = indent_str + "Scan(table=" + scan_plan->tab_name_ + ")\n";
            }
            break;
        }
        case T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
            std::vector<std::string> conditions;
            std::set<std::string> table_set; // 使用set来收集表名

            // 收集所有涉及的表名
            collect_tables(join_plan, table_set);

            // 将set中的表名转换为vector
            std::vector<std::string> tables(table_set.begin(), table_set.end());

            // 收集连接条件
            for (const auto &cond : join_plan->conds_)
            {
                conditions.push_back(format_condition(cond));
            }

            result = indent_str + "Join(tables=" + format_list(tables) +
                     ", condition=" + format_list(conditions) + ")\n";
            result += plan_to_string(join_plan->left_, indent + 1);
            result += plan_to_string(join_plan->right_, indent + 1);
            break;
        }
        default:
            result = indent_str + "Unknown\n";
            break;
        }
        return result;
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (done_)
        {
            return nullptr;
        }
        done_ = true;

        std::string plan_str = plan_to_string(plan_);
        int len = plan_str.length();
        auto record = std::make_unique<RmRecord>(len);
        memcpy(record->data, plan_str.c_str(), len);
        return record;
    }

    // 辅助函数：将列表按字典序排序并格式化
    std::string format_list(const std::vector<std::string> &items)
    {
        std::vector<std::string> sorted_items = items;
        std::sort(sorted_items.begin(), sorted_items.end());
        std::stringstream ss;
        for (size_t i = 0; i < sorted_items.size(); ++i)
        {
            if (i > 0)
                ss << ",";
            ss << sorted_items[i];
        }
        return ss.str();
    }

    // 辅助函数：格式化TabCol为字符串
    std::string format_tabcol(const TabCol &col)
    {
        return col.tab_name + "." + col.col_name;
    }

    // 辅助函数：格式化条件为字符串
    std::string format_condition(const Condition &cond)
    {
        std::stringstream ss;
        ss << format_tabcol(cond.lhs_col);
        switch (cond.op)
        {
        case OP_EQ:
            ss << "=";
            break;
        case OP_NE:
            ss << "<>";
            break;
        case OP_LT:
            ss << "<";
            break;
        case OP_GT:
            ss << ">";
            break;
        case OP_LE:
            ss << "<=";
            break;
        case OP_GE:
            ss << ">=";
            break;
        default:
            ss << "unknown_op";
            break;
        }
        if (cond.is_rhs_val)
        {
            // 修改：直接调用 get_value_string 函数，确保字符串/日期值有引号
            ss << get_value_string(cond.rhs_val);
        }
        else
        {
            ss << format_tabcol(cond.rhs_col);
        }
        return ss.str();
    }
};