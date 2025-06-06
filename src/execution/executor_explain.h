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

    // 递归生成查询计划树的字符串表示
    std::string explain_plan(const std::shared_ptr<Plan> &plan, int depth = 0)
    {
        std::cout << "[Debug] Explaining plan at depth " << depth << std::endl;
        std::cout << "[Debug] Plan type: " << plan->tag << std::endl;

        std::string result;
        std::string indent(depth * 4, ' '); // 使用4个空格作为一个缩进级别

        switch (plan->tag)
        {
        case T_select:
        {
            auto select_plan = std::static_pointer_cast<DMLPlan>(plan);
            if (select_plan->subplan_)
            {
                result += explain_plan(select_plan->subplan_, depth);
            }
            break;
        }
        case T_Projection:
        {
            std::cout << "[Debug] Processing Projection node" << std::endl;
            auto proj_plan = std::static_pointer_cast<ProjectionPlan>(plan);
            std::string indent_str(depth, '\t');
            result += indent_str + "Project(columns=[";
            bool first = true;
            std::cout << "[Debug] Number of columns: " << proj_plan->sel_cols_.size() << std::endl;

            // 对列名进行排序
            std::vector<std::string> columns;
            for (const auto &col : proj_plan->sel_cols_)
            {
                std::cout << "[Debug] Column: " << col.tab_name << "." << col.col_name << std::endl;
                columns.push_back(col.tab_name + "." + col.col_name);
            }
            std::sort(columns.begin(), columns.end());

            result += columns[0];
            for (size_t i = 1; i < columns.size(); i++)
            {
                result += "," + columns[i];
            }
            result += "])\n";
            if (proj_plan->subplan_)
            {
                std::cout << "[Debug] Processing projection subplan" << std::endl;
                result += explain_plan(proj_plan->subplan_, depth + 1);
            }
            break;
        }
        case T_SeqScan:
        case T_IndexScan:
        {
            std::cout << "[Debug] Processing Scan node" << std::endl;
            auto scan_plan = std::static_pointer_cast<ScanPlan>(plan);
            std::cout << "[Debug] Table name: " << scan_plan->tab_name_ << std::endl;
            std::cout << "[Debug] Number of conditions: " << scan_plan->fed_conds_.size() << std::endl;

            if (!scan_plan->fed_conds_.empty())
            {
                // 先对条件按字典序排序
                std::vector<std::string> conditions;
                for (const auto &cond : scan_plan->fed_conds_)
                {
                    std::string condition;
                    // 构建左侧表达式
                    condition += cond.lhs_col.tab_name + "." + cond.lhs_col.col_name;

                    // 添加操作符
                    switch (cond.op)
                    {
                    case OP_EQ:
                        condition += "=";
                        break;
                    case OP_NE:
                        condition += "<>";
                        break;
                    case OP_LT:
                        condition += "<";
                        break;
                    case OP_GT:
                        condition += ">";
                        break;
                    case OP_LE:
                        condition += "<=";
                        break;
                    case OP_GE:
                        condition += ">=";
                        break;
                    default:
                        throw InternalError("Unsupported operator type");
                    }

                    // 构建右侧表达式
                    if (cond.is_rhs_val)
                    {
                        // 如果右侧是值
                        switch (cond.rhs_val.type)
                        {
                        case TYPE_INT:
                            condition += std::to_string(cond.rhs_val.int_val);
                            break;
                        case TYPE_FLOAT:
                            condition += std::to_string(cond.rhs_val.float_val);
                            break;
                        case TYPE_STRING:
                            condition += cond.rhs_val.str_val;
                            break;
                        default:
                            throw InternalError("Unsupported value type");
                        }
                    }
                    else
                    {
                        // 如果右侧是列
                        condition += cond.rhs_col.tab_name + "." + cond.rhs_col.col_name;
                    }
                    conditions.push_back(condition);
                }
                // 按字典序排序条件
                std::sort(conditions.begin(), conditions.end());

                std::string indent_str(depth, '\t'); // 使用制表符而不是空格
                result += indent_str + "Filter(condition=[" + conditions[0];
                for (size_t i = 1; i < conditions.size(); i++)
                {
                    result += "," + conditions[i];
                }
                result += "])\n";
                result += indent_str + "\tScan(table=" + scan_plan->tab_name_ + ")\n";
            }
            else
            {
                std::string indent_str(depth, '\t');
                result += indent_str + "Scan(table=" + scan_plan->tab_name_ + ")\n";
            }
            break;
        }
        case T_NestLoop:
        {
            auto join_plan = std::static_pointer_cast<JoinPlan>(plan);
            std::string indent_str(depth, '\t');
            result += indent_str + "Join(tables=[";
            // 收集所有涉及的表名
            std::set<std::string> tables;
            collect_tables(join_plan, tables);

            // 将表名转换为vector并排序
            std::vector<std::string> sorted_tables(tables.begin(), tables.end());
            std::sort(sorted_tables.begin(), sorted_tables.end());

            result += sorted_tables[0];
            for (size_t i = 1; i < sorted_tables.size(); i++)
            {
                result += "," + sorted_tables[i];
            }
            result += "], condition=[";

            // 对连接条件进行排序
            std::vector<std::string> conditions;
            for (const auto &cond : join_plan->conds_)
            {
                conditions.push_back(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name + "=" +
                                     cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
            }
            std::sort(conditions.begin(), conditions.end());

            if (!conditions.empty())
            {
                result += conditions[0];
                for (size_t i = 1; i < conditions.size(); i++)
                {
                    result += "," + conditions[i];
                }
            }
            result += "])\n";
            result += explain_plan(join_plan->left_, depth + 1);
            result += explain_plan(join_plan->right_, depth + 1);
            break;
        }
        default:
            std::cout << "[Debug] Unknown plan type: " << plan->tag << std::endl;
            throw InternalError("Unexpected plan type");
        }
        return result;
    }

    // 收集连接计划中涉及的所有表名
    void collect_tables(const std::shared_ptr<JoinPlan> &plan, std::set<std::string> &tables)
    {
        if (auto scan_left = std::dynamic_pointer_cast<ScanPlan>(plan->left_))
        {
            tables.insert(scan_left->tab_name_);
        }
        else if (auto join_left = std::dynamic_pointer_cast<JoinPlan>(plan->left_))
        {
            collect_tables(join_left, tables);
        }

        if (auto scan_right = std::dynamic_pointer_cast<ScanPlan>(plan->right_))
        {
            tables.insert(scan_right->tab_name_);
        }
        else if (auto join_right = std::dynamic_pointer_cast<JoinPlan>(plan->right_))
        {
            collect_tables(join_right, tables);
        }
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
                result_ = this->explain_plan(explain_plan->subplan_);
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
            for (const auto &col : proj_plan->sel_cols_)
            {
                cols.push_back(format_tabcol(col));
            }
            if (cols.empty())
            {
                result = indent_str + "Project(columns=[*])\n";
            }
            else
            {
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
        ss << "[";
        for (size_t i = 0; i < sorted_items.size(); ++i)
        {
            if (i > 0)
                ss << ",";
            ss << sorted_items[i];
        }
        ss << "]";
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
            switch (cond.rhs_val.type)
            {
            case TYPE_INT:
                ss << cond.rhs_val.int_val;
                break;
            case TYPE_FLOAT:
                ss << cond.rhs_val.float_val;
                break;
            case TYPE_STRING:
                ss << cond.rhs_val.str_val;
                break;
            default:
                ss << "unknown_type";
                break;
            }
        }
        else
        {
            ss << format_tabcol(cond.rhs_col);
        }
        return ss.str();
    }
};