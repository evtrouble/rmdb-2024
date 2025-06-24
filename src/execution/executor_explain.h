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
#include <iomanip>

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

    // 获取表的显示名称（处理别名）
    std::string getTableDisplayName(const std::string &original_name, const std::shared_ptr<ast::SelectStmt> &stmt, bool use_alias = false, bool use_raw = false)
    {
        if (!stmt)
            return original_name;
        if (use_raw)
            return original_name;
        // 查找表别名
        if (use_alias)
        {
            for (size_t i = 0; i < stmt->tabs.size(); i++)
            {
                if (stmt->tabs[i] == original_name && i < stmt->tab_aliases.size() && !stmt->tab_aliases[i].empty())
                {
                    return stmt->tab_aliases[i];
                }
            }
        }
        return original_name;
    }

    // 格式化列名（确保包含表名前缀）
    std::string formatColumnName(const TabCol &col, const std::shared_ptr<ast::SelectStmt> &stmt, bool use_alias = false, bool use_raw = true)
    {
        std::string table_name = getTableDisplayName(col.tab_name, stmt, use_alias, use_raw);
        return table_name + "." + col.col_name;
    }

    // 格式化条件字符串
    std::string formatCondition(const Condition &cond, const std::shared_ptr<ast::SelectStmt> &stmt, bool use_alias = false, bool use_raw = true)
    {
        std::string result;

        // 左侧列名（必须包含表名前缀）
        result += formatColumnName(cond.lhs_col, stmt, use_alias, use_raw);

        // 操作符
        result += getOpString(cond.op);

        // 右侧值或列名
        if (cond.is_rhs_val)
        {
            result += getValueString(cond.rhs_val, true);
        }
        else
        {
            result += formatColumnName(cond.rhs_col, stmt, use_alias, use_raw);
        }

        return result;
    }

    // 收集计划树中的所有表名
    void collectTableNames(const std::shared_ptr<Plan> &plan, std::set<std::string> &table_set, const std::shared_ptr<ast::SelectStmt> &stmt)
    {
        if (!plan)
            return;

        switch (plan->tag)
        {
        case T_SeqScan:
        case T_IndexScan:
        {
            auto scan_plan = std::static_pointer_cast<ScanPlan>(plan);
            std::string display_name = getTableDisplayName(scan_plan->tab_name_, stmt);
            table_set.insert(display_name);
            break;
        }
        case T_Projection:
        {
            auto proj_plan = std::static_pointer_cast<ProjectionPlan>(plan);
            collectTableNames(proj_plan->subplan_, table_set, stmt);
            break;
        }
        case T_NestLoop:
        case T_SortMerge:
        {
            auto join_plan = std::static_pointer_cast<JoinPlan>(plan);
            collectTableNames(join_plan->left_, table_set, stmt);
            collectTableNames(join_plan->right_, table_set, stmt);
            break;
        }
        case T_Filter:
        {
            auto filter_plan = std::static_pointer_cast<FilterPlan>(plan);
            collectTableNames(filter_plan->subplan_, table_set, stmt);
            break;
        }
        case T_Select:
        {
            auto select_plan = std::static_pointer_cast<DMLPlan>(plan);
            collectTableNames(select_plan->subplan_, table_set, stmt);
            break;
        }
        default:
            break;
        }
    }

    // 格式化字符串列表（按字母顺序排序）
    std::string formatSortedList(const std::vector<std::string> &items)
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

    // 递归生成查询计划树的字符串表示
    std::string explain_plan(const std::shared_ptr<Plan> &plan, int depth = 0, std::shared_ptr<ast::SelectStmt> stmt = nullptr)
    {
        if (!plan)
            return "";

        std::string result;
        std::string indent_str(depth, '\t');

        switch (plan->tag)
        {
        case T_Select:
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

            // 处理Project节点 - 严格按照要求格式化
            std::string proj_str = "Project(columns=";

            // 检查是否是SELECT *查询
            if (stmt && stmt->is_select_star_query())
            {
                proj_str += "[*])";
            }
            else if (proj_plan->sel_cols_.empty())
            {
                // 如果没有指定列，默认为SELECT *
                proj_str += "[*])";
            }
            else
            {
                std::vector<std::string> columns;
                bool has_star = false;

                for (const auto &col : proj_plan->sel_cols_)
                {
                    if (col.col_name == "*")
                    {
                        has_star = true;
                        break;
                    }
                    else
                    {
                        // 确保列名包含表名前缀
                        columns.push_back(formatColumnName(col, stmt, true, false));
                    }
                }

                if (has_star)
                {
                    proj_str += "[*])";
                }
                else
                {
                    // 按字母顺序排序列名
                    proj_str += "[" + formatSortedList(columns) + "])";
                }
            }

            result += indent_str + proj_str + "\n";

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
            std::string display_name = getTableDisplayName(scan_plan->tab_name_, stmt, false);

            // 如果有过滤条件，先添加Filter节点
            if (!scan_plan->fed_conds_.empty())
            {
                std::vector<std::string> conditions;
                for (const auto &cond : scan_plan->fed_conds_)
                {
                    // 确保条件中的列名包含表名前缀
                    conditions.push_back(formatCondition(cond, stmt));
                }

                // Filter节点 - 条件按字母顺序排序
                result += indent_str + "Filter(condition=[" + formatSortedList(conditions) + "])\n";

                // Scan节点
                std::string scan_type = (plan->tag == T_SeqScan) ? "Scan" : "IndexScan";
                result += std::string(depth + 1, '\t') + scan_type + "(table=" + display_name + ")\n";
            }
            else
            {
                std::string scan_type = (plan->tag == T_SeqScan) ? "Scan" : "IndexScan";
                result += indent_str + scan_type + "(table=" + display_name + ")\n";
            }
            break;
        }
        case T_NestLoop:
        case T_SortMerge:
        {
            auto join_plan = std::static_pointer_cast<JoinPlan>(plan);

            // 收集所有涉及的表名
            std::set<std::string> table_set;
            collectTableNames(join_plan, table_set, stmt);
            std::vector<std::string> tables(table_set.begin(), table_set.end());

            // 收集连接条件
            std::vector<std::string> conditions;
            for (const auto &cond : join_plan->conds_)
            {
                if (!cond.is_rhs_val) // 只处理列与列之间的连接条件
                {
                    conditions.push_back(formatCondition(cond, stmt, true, false));
                }
            }

            // Join节点 - 表名和条件都按字母顺序排序
            result += indent_str + "Join(tables=[" + formatSortedList(tables) + "]";
            if (!conditions.empty())
            {
                result += ",condition=[" + formatSortedList(conditions) + "]";
            }
            result += ")\n";

            // 收集所有子节点并按优先级排序
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

            // 收集过滤条件
            std::vector<std::string> conditions;
            for (const auto &cond : filter_plan->conds_)
            {
                // 确保条件中的列名包含表名前缀
                conditions.push_back(formatCondition(cond, stmt, true, false));
            }

            // Filter节点 - 条件按字母顺序排序
            result += indent_str + "Filter(condition=[" + formatSortedList(conditions) + "])\n";

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
    std::string getOpString(CompOp op)
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
    std::string getValueString(const Value &val, bool use_raw_float = false)
    {
        std::stringstream ss;
        switch (val.type)
        {
        case TYPE_INT:
            ss << val.int_val;
            break;
        case TYPE_FLOAT:
            // if (use_raw_float)
            // {
            //     // 输出原始浮点值
            //     ss << val.float_val;
            // }
            // else
            // {
            // 输出六位精度格式化值
            ss << std::fixed << std::setprecision(FLOAT_PRECISION) << val.float_val;
            // }
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

    ExecutionType type() const override { return ExecutionType::Explain; }

    // 保留原有的兼容性方法
    std::string format_list(const std::vector<std::string> &items)
    {
        return formatSortedList(items);
    }

    std::string format_tabcol(const TabCol &col)
    {
        return col.tab_name + "." + col.col_name;
    }

    void collect_tables(const std::shared_ptr<Plan> &plan, std::set<std::string> &table_set)
    {
        collectTableNames(plan, table_set, nullptr);
    }
};