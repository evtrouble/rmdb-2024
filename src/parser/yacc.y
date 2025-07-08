%{
#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>
#include <cstring>
#include <cmath>
#include <sstream>
#include <iomanip>

// 浮点数精度常量
const int FLOAT_PRECISION = 6;
const float FLOAT_PRECISION_MULTIPLIER = std::pow(10, FLOAT_PRECISION);

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    //std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
        std::string error_msg = "Parser Error at line " + std::to_string(locp->first_line) +
                           " column " + std::to_string(locp->first_column) + ": " + s + "\n";
    std::cerr << error_msg;

    // 将错误信息保存到全局变量中，以便后续传递给客户端
    extern char *g_error_msg;
    if (g_error_msg != nullptr) {
        strcpy(g_error_msg, error_msg.c_str());
    }
}

using namespace ast;
%}

// request a pure (reentrant) parser
%define api.pure full
// enable location in error handler
%locations
// enable verbose syntax error message
%define parse.error verbose

// keywords
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC ORDER GROUP BY HAVING LIMIT
WHERE UPDATE SET SELECT INT CHAR FLOAT DATETIME INDEX AND SEMI JOIN ON IN NOT EXIT HELP DIV EXPLAIN
TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ORDER_BY ENABLE_NESTLOOP ENABLE_SORTMERGE STATIC_CHECKPOINT
SUM COUNT MAX MIN AVG AS LOAD
// non-keywords
%token LEQ NEQ GEQ T_EOF
%token OUTPUT_FILE OFF


// type-specific tokens
%token <sv_str> IDENTIFIER VALUE_STRING VALUE_PATH
%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT
%token <sv_bool> VALUE_BOOL

// specify types for non-terminal symbol
%type <sv_node> stmt dbStmt ddl dml txnStmt setStmt io_stmt
%type <sv_field> field
%type <sv_fields> fieldList
%type <sv_type_len> type
%type <sv_comp_op> op
%type <sv_expr> expr
%type <sv_val> value
%type <sv_vals> valueList
%type <sv_str> tbName colName ALIAS fileName
%type <sv_strs> colNameList
%type <sv_col> col aggCol
%type <sv_cols> colList selector opt_groupby_clause
%type <sv_set_clause> setClause
%type <sv_set_clauses> setClauses
%type <sv_cond> condition
%type <sv_conds> whereClause optWhereClause opt_having_clause optJoinClause
%type <sv_orderby>  order_clause opt_order_clause
%type <sv_int> opt_limit_clause
%type <sv_orderby_dir> opt_asc_desc
%type <sv_setKnobType> set_knob_type
%type <sv_order_item> order_item
%type <sv_table_list> tableList
%%
start:
        stmt ';'
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |  io_stmt
    {
        parse_tree = $1;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    |   setStmt
    |   EXPLAIN dml
    {
        $$ = std::make_shared<ExplainStmt>(std::move($2));
    }
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   LOAD fileName INTO tbName
    {
        $$ = std::make_shared<LoadStmt>(std::move($2), std::move($4));
    }
    ;

setStmt:
        SET set_knob_type '=' VALUE_BOOL
    {
        $$ = std::make_shared<SetStmt>($2, $4);  // 移除std::move
    }
    ;
io_stmt:
        SET OUTPUT_FILE ON
    {
        $$ = std::make_shared<IoEnable>(true);
    }
    |   SET OUTPUT_FILE OFF
    {
        $$ = std::make_shared<IoEnable>(false);
    }
    ;
ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = std::make_shared<CreateTable>(std::move($3), std::move($5));
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>(std::move($3));
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>(std::move($2));
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<CreateIndex>(std::move($3), std::move($5));
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<DropIndex>(std::move($3), std::move($5));
    }
    |   SHOW INDEX FROM tbName
    {
        $$ = std::make_shared<ShowIndex>(std::move($4));
    }
    |   CREATE STATIC_CHECKPOINT
    {
        $$ = std::make_shared<CreateStaticCheckpoint>();
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>(std::move($3), std::move($6));
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>(std::move($3), std::move($4));
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>(std::move($2), std::move($4), std::move($5));
    }
    |   SELECT selector FROM tableList optWhereClause opt_groupby_clause opt_having_clause opt_order_clause opt_limit_clause
    {
        // 例如在 SelectStmt 创建时
        $$ = std::make_shared<SelectStmt>(
            std::move($2),             // selector
            std::move($4.tables),      // 表列表
            std::move($4.jointree),    // 连接树
            std::move($5),             // where条件
            std::move($6),             // groupby
            std::move($7),             // having
            std::move($8),             // order
            $9,                        // limit (int，不需要move)
            std::move($4.aliases)      // 表别名
        );
    }
    ;

fieldList:
        field
    {
        $$ = std::vector<std::shared_ptr<Field>>{std::move($1)};
    }
    |   fieldList ',' field
    {
        $$.emplace_back(std::move($3));
    }
    ;

colNameList:
        colName
    {
        $$ = std::vector<std::string>{std::move($1)}; // 使用 move
    }
    | colNameList ',' colName
    {
        $$.emplace_back(std::move($3)); // 使用 move
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>(std::move($1), std::move($2));
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    |   DATETIME
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_DATETIME, 19);
    }
    ;

valueList:
        value
    {
        $$ = std::vector<std::shared_ptr<Value>>{std::move($1)}; // 使用 move
    }
    |   valueList ',' value
    {
        $$.emplace_back(std::move($3)); // 使用 move
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<IntLit>($1);
    }
    |   VALUE_FLOAT
    {
        // 浮点数在词法分析阶段已经进行了精度处理
        $$ = std::make_shared<FloatLit>($1);
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>(std::move($1));
    }
    |   VALUE_BOOL
    {
        $$ = std::make_shared<BoolLit>($1);
    }
    ;

condition:
        col op expr
    {
        $$ = std::make_shared<BinaryExpr>(std::move($1), $2, std::move($3));
    }
    ;


optWhereClause:
        /* epsilon */ { /* ignore*/ }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

optJoinClause:
        /* epsilon */ { /* ignore*/ }
    |   ON whereClause
    {
        $$ = $2;
    }
    ;

opt_having_clause:
    /* epsilon */ { /* ignore*/ }
    |   HAVING whereClause
    {
        $$ = $2;
    }
    ;

whereClause:
        condition
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{std::move($1)}; // 使用 move
    }
    |   whereClause AND condition
    {
        $$.emplace_back(std::move($3)); // 使用 move
    }
    ;


col:
        tbName '.' colName
    {
        $$ = std::make_shared<Col>(std::move($1), std::move($3));
    }
    |   colName
    {
        $$ = std::make_shared<Col>("", std::move($1));
    }
    |   colName AS ALIAS
    {
        $$ = std::make_shared<Col>("", std::move($1));
        $$->alias = std::move($3);
    }
    |   aggCol AS ALIAS
    {
        $$ = std::move($1);
        $$->alias = std::move($3);
    }
    ;

aggCol:
    SUM '(' col ')'
{
    $$ = std::make_shared<Col>(std::move($3->tab_name), std::move($3->col_name), AggFuncType::SUM);
}
    |   MIN '(' col ')'
    {
        // 优化后
        $$ = std::make_shared<Col>(std::move($3->tab_name), std::move($3->col_name), AggFuncType::MIN);
    }
    |   MAX '(' col ')'
    {
        $$ = std::make_shared<Col>(std::move($3->tab_name), std::move($3->col_name), AggFuncType::MAX);
    }
    |   AVG '(' col ')'
    {
        $$ = std::make_shared<Col>(std::move($3->tab_name), std::move($3->col_name), AggFuncType::AVG);
    }
    |   COUNT '(' col ')'
    {
        $$ = std::make_shared<Col>(std::move($3->tab_name), std::move($3->col_name), AggFuncType::COUNT);
    }
    |   COUNT '(' '*' ')'
    {
        $$ = std::make_shared<Col>("", "*", AggFuncType::COUNT);
    }
    ;

colList:
        col
    {
        $$ = std::vector<std::shared_ptr<Col>>{std::move($1)}; // 使用 move
    }
    |   colList ',' col
    {
        $$.emplace_back(std::move($3)); // 使用 move
    }
    ;

op:
        '='
    {
        $$ = SV_OP_EQ;
    }
    |   '<'
    {
        $$ = SV_OP_LT;
    }
    |   '>'
    {
        $$ = SV_OP_GT;
    }
    |   NEQ
    {
        $$ = SV_OP_NE;
    }
    |   LEQ
    {
        $$ = SV_OP_LE;
    }
    |   GEQ
    {
        $$ = SV_OP_GE;
    }
    |   IN
    {
	    $$ = SV_OP_IN;
    }
    |   NOT IN
    {
    	$$ = SV_OP_NOT_IN;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{std::move($1)}; // 使用 move
    }
    |   setClauses ',' setClause
    {
        $$.emplace_back(std::move($3)); // 使用 move
    }
    ;

setClause:
        colName '=' value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($3), UpdateOp::ASSINGMENT);
    }
    |   colName '=' colName value
    {
        $$ = std::make_shared<SetClause>($1, $4, UpdateOp::SELF_ADD);
    }
    |   colName '=' colName '+' value  // 修复：移除多余的参数
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), UpdateOp::SELF_ADD);
    }
    |   colName '=' colName '-' value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), UpdateOp::SELF_SUB);
    }
    |   colName '=' colName '*' value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), UpdateOp::SELF_MUT);
    }
    |   colName '=' colName DIV value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), UpdateOp::SELF_DIV);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   colList
    ;

tableList:
        tbName
    {
        $$.tables = {std::move($1)}; // 使用 move
        $$.aliases = {""};
        $$.jointree = {};
    }
    |   tbName ALIAS
    {
        $$.tables = {std::move($1)}; // 使用 move
        $$.aliases = {std::move($2)}; // 使用 move
        $$.jointree = {};
    }
    |   tableList ',' tbName
    {
        $$.tables = std::move($1.tables); // 使用 move
        $$.aliases = std::move($1.aliases); // 使用 move
        $$.tables.emplace_back(std::move($3)); // 使用 move
        $$.aliases.emplace_back("");
        $$.jointree = std::move($1.jointree); // 使用 move
    }
    |   tableList ',' tbName ALIAS
    {
        $$.tables = std::move($1.tables);     // 使用 move
        $$.aliases = std::move($1.aliases);   // 使用 move
        $$.tables.emplace_back(std::move($3)); // 使用 move
        $$.aliases.emplace_back(std::move($4)); // 使用 move
        $$.jointree = std::move($1.jointree);  // 使用 move
    }
    |   tableList JOIN tbName optJoinClause
    {
        auto join_expr = std::make_shared<JoinExpr>(
            std::move($1.tables.back()),  // left
            std::move($3),                // right
            std::move($4),                // conds
            INNER_JOIN,                   // type
            "",                           // left_alias
            ""                            // right_alias
        );
        $$.tables = std::move($1.tables);
        $$.aliases = std::move($1.aliases);
        $$.tables.emplace_back(std::move($3));
        $$.aliases.emplace_back("");
        $$.jointree = std::move($1.jointree);
        $$.jointree.emplace_back(std::move(join_expr));
    }
    |   tableList JOIN tbName ALIAS optJoinClause
    {
        auto join_expr = std::make_shared<JoinExpr>(
            std::move($1.tables.back()),  // left
            std::move($3),                // right
            std::move($5),                // conds
            INNER_JOIN,                   // type
            "",                           // left_alias
            std::move($4)                 // right_alias
        );
        $$.tables = std::move($1.tables);
        $$.aliases = std::move($1.aliases);
        $$.tables.emplace_back(std::move($3));
        $$.aliases.emplace_back(std::move($4));
        $$.jointree = std::move($1.jointree);
        $$.jointree.emplace_back(std::move(join_expr));
    }
    |   tableList SEMI JOIN tbName optJoinClause
    {
        auto join_expr = std::make_shared<JoinExpr>(
            std::move($1.tables.back()),  // left
            std::move($4),                // right
            std::move($5),                // conds
            SEMI_JOIN,                    // type
            "",                           // left_alias
            ""                            // right_alias
        );
        $$.tables = std::move($1.tables);
        $$.aliases = std::move($1.aliases);
        $$.tables.emplace_back(std::move($4));
        $$.aliases.emplace_back("");
        $$.jointree = std::move($1.jointree);
        $$.jointree.emplace_back(std::move(join_expr));
    }
    |   tableList SEMI JOIN tbName ALIAS optJoinClause
    {
        auto join_expr = std::make_shared<JoinExpr>(
            std::move($1.tables.back()),  // left
            std::move($4),                // right
            std::move($6),                // conds
            SEMI_JOIN,                    // type
            "",                           // left_alias
            std::move($5)                 // right_alias
        );
        $$.tables = std::move($1.tables);
        $$.aliases = std::move($1.aliases);
        $$.tables.emplace_back(std::move($4));
        $$.aliases.emplace_back(std::move($5));
        $$.jointree = std::move($1.jointree);
        $$.jointree.emplace_back(std::move(join_expr));
    }
    ;

opt_order_clause:
    ORDER BY order_clause
    {
        $$ = $3;
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

opt_limit_clause:
    LIMIT VALUE_INT
    {
        $$ = $2;
    }
    |   /* epsilon */
    {
        $$ = -1;
    }
    ;

opt_groupby_clause:
    GROUP BY colList
    {
        $$ = $3;
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

order_clause:
      order_item
    {
        $$ = std::make_shared<OrderBy>(std::move($1.first), $1.second);
    }
    |   order_clause ',' order_item
    {
        $1->addItem(std::move($3.first), $3.second);
        $$ = std::move($1);  // 使用 move
    }
    ;

order_item:
      col opt_asc_desc
    {
        $$ = std::make_pair(std::move($1), $2);
    }
    ;

opt_asc_desc:
    ASC          { $$ = OrderBy_ASC;     }
    |  DESC      { $$ = OrderBy_DESC;    }
    |       { $$ = OrderBy_DEFAULT; }
    ;

set_knob_type:
    ENABLE_NESTLOOP { $$ = ast::SetKnobType::EnableNestLoop; }
    |   ENABLE_SORTMERGE { $$ = ast::SetKnobType::EnableSortMerge; }
    ;

tbName: IDENTIFIER;

colName: IDENTIFIER;

ALIAS: IDENTIFIER;
fileName: VALUE_PATH;



%%