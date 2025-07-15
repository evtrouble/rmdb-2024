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

%destructor { delete $$; } <sv_str>
%destructor { delete $$; } <sv_strs>
%destructor { delete $$; } <sv_type_len>
%destructor { delete $$; } <sv_field>
%destructor { delete $$; } <sv_fields>
%destructor { delete $$; } <sv_expr>
%destructor { delete $$; } <sv_val>
%destructor { delete $$; } <sv_vals>
%destructor { delete $$; } <sv_col>
%destructor { delete $$; } <sv_cols>
%destructor { delete $$; } <sv_set_clause>
%destructor { delete $$; } <sv_set_clauses>
%destructor { delete $$; } <sv_cond>
%destructor { delete $$; } <sv_conds>
%destructor { delete $$; } <sv_orderby>
%destructor { delete $$; } <sv_order_item>
%destructor { delete $$; } <sv_table_list>

// request a pure (reentrant) parser
%define api.pure full
// enable location in error handler
%locations
// enable verbose syntax error message
%define parse.error verbose

%define lr.default-reduction most
%define lr.keep-unreachable-state false
%define lr.type ielr

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
        parse_tree = std::unique_ptr<ast::TreeNode>($1);
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_unique<Help>();
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
        parse_tree = std::unique_ptr<ast::TreeNode>($1);
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
        $$ = new ExplainStmt($2);
    }
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = new TxnBegin;
    }
    |   TXN_COMMIT
    {
        $$ = new TxnCommit;
    }
    |   TXN_ABORT
    {
        $$ = new TxnAbort;
    }
    | TXN_ROLLBACK
    {
        $$ = new TxnRollback;
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = new ShowTables;
    }
    |   LOAD fileName INTO tbName
    {
        $$ = new LoadStmt(*$2, *$4);
        delete $2;
        delete $4;
    }
    ;

setStmt:
        SET set_knob_type '=' VALUE_BOOL
    {
        $$ = new SetStmt($2, $4);
    }
    ;
io_stmt:
        SET OUTPUT_FILE ON
    {
        $$ = new IoEnable(true);
    }
    |   SET OUTPUT_FILE OFF
    {
        $$ = new IoEnable(false);
    }
    ;
ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = new CreateTable(*$3, *$5);
        delete $3;
        delete $5;
    }
    |   DROP TABLE tbName
    {
        $$ = new DropTable(*$3);
        delete $3;
    }
    |   DESC tbName
    {
        $$ = new DescTable(*$2);
        delete $2;
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = new CreateIndex(*$3, *$5);
        delete $3;
        delete $5;
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = new DropIndex(*$3, *$5);
        delete $3;
        delete $5;
    }
    |   SHOW INDEX FROM tbName
    {
        $$ = new ShowIndex(*$4);
        delete $4;
    }
    |   CREATE STATIC_CHECKPOINT
    {
        $$ = new CreateStaticCheckpoint;
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = new InsertStmt(*$3, *$6);
        delete $3;
        delete $6;
    }
    |   DELETE FROM tbName optWhereClause
    {
        DeleteStmt* delete_stmt = new DeleteStmt(*$3);
        if($4 != nullptr) {
            delete_stmt->conds = std::move(*$4);
            delete $4;
        }
        delete $3;
        $$ = delete_stmt;
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        UpdateStmt* update_stmt = new UpdateStmt(*$2, *$4);
        if($5 != nullptr) {
            update_stmt->conds = std::move(*$5);
            delete $5;
        }
        delete $4;
        delete $2;
        $$ = update_stmt;
    }
    |   SELECT selector FROM tableList optWhereClause opt_groupby_clause opt_having_clause opt_order_clause opt_limit_clause
    {
        // 例如在 SelectStmt 创建时
        SelectStmt* select_stmt = new SelectStmt(
            *$2,             // selector
            $4->tables,      // 表列表
            $4->jointree,    // 连接树
            $9               // limit (int，不需要move)
        );
        select_stmt->tab_aliases = std::move($4->aliases);      // 表别名
        if($5 != nullptr) {
            select_stmt->conds = std::move(*$5);
            delete $5;
        }
        if($6 != nullptr) {
            select_stmt->groupby = std::move(*$6);
            delete $6;
        }
        if($7 != nullptr) {
            select_stmt->having_conds = std::move(*$7);
            delete $7;
        }
        if($8 != nullptr) {
            select_stmt->order = std::move(*$8);
            delete $8;
        }
        delete $2;
        delete $4;
        $$ = select_stmt;
    }
    ;

fieldList:
        field
    {
        $$ = new std::vector<std::unique_ptr<Field>>();
        $$->emplace_back($1);
    }
    |   fieldList ',' field
    {
        $$ = $1;
        $$->emplace_back($3);
    }
    ;

colNameList:
        colName
    {
        $$ = new std::vector<std::string>(); // 使用 move
        $$->emplace_back(std::move(*$1));
        delete $1;
    }
    | colNameList ',' colName
    {
        $$ = $1;
        $$->emplace_back(std::move(*$3)); // 使用 move
        delete $3;
    }
    ;

field:
        colName type
    {
        $$ = new ColDef(*$1, *$2);
        delete $1;
        delete $2;
    }
    ;

type:
        INT
    {
        $$ = new TypeLen(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = new TypeLen(SV_TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = new TypeLen(SV_TYPE_FLOAT, sizeof(float));
    }
    |   DATETIME
    {
        $$ = new TypeLen(SV_TYPE_DATETIME, 19);
    }
    ;

valueList:
        value
    {
        $$ = new std::vector<std::unique_ptr<Value>>();
        $$->emplace_back($1);
    }
    |   valueList ',' value
    {
        $$ = $1;
        $$->emplace_back($3); // 使用 move
    }
    ;

value:
        VALUE_INT
    {
        $$ = new IntLit($1);
    }
    |   VALUE_FLOAT
    {
        // 浮点数在词法分析阶段已经进行了精度处理
        $$ = new FloatLit($1);
    }
    |   VALUE_STRING
    {
        $$ = new StringLit(*$1);
        delete $1;
    }
    |   VALUE_BOOL
    {
        $$ = new BoolLit($1);
    }
    ;

condition:
        col op expr
    {
        $$ = new BinaryExpr(*$1, $2, $3);
        delete $1;
    }
    ;


optWhereClause:
        /* epsilon */ { $$=nullptr; }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

optJoinClause:
        /* epsilon */ { $$=nullptr; }
    |   ON whereClause
    {
        $$ = $2;
    }
    ;

opt_having_clause:
    /* epsilon */ { $$=nullptr; }
    |   HAVING whereClause
    {
        $$ = $2;
    }
    ;

whereClause:
        condition
    {
        $$ = new std::vector<BinaryExpr>(); // 使用 move
        $$->emplace_back(std::move(*$1));
        delete $1;
    }
    |   whereClause AND condition
    {
        $$ = $1;
        $$->emplace_back(std::move(*$3)); // 使用 move
        delete $3;
    }
    ;


col:
        tbName '.' colName
    {
        $$ = new Col(*$1, *$3);
        delete $1;
        delete $3;
    }
    |   colName
    {
        $$ = new Col(*$1);
        delete $1;
    }
    |   colName AS ALIAS
    {
        $$ = new Col(*$1);
        $$->alias = std::move(*$3);
        delete $1;
        delete $3;
    }
    |   aggCol AS ALIAS
    {
        $$ = $1;
        $$->alias = std::move(*$3);
        delete $3;
    }
    |   aggCol
    {
        $$ = $1;
    }
    ;

aggCol:
        SUM '(' col ')'
    {
        $$ = $3;
        $$->agg_type = AggFuncType::SUM;
    }
    |   MIN '(' col ')'
    {
        $$ = $3;
        $$->agg_type = AggFuncType::MIN;
    }
    |   MAX '(' col ')'
    {
        $$ = $3;
        $$->agg_type = AggFuncType::MAX;
    }
    |   AVG '(' col ')'
    {
        $$ = $3;
        $$->agg_type = AggFuncType::AVG;
    }
    |   COUNT '(' col ')'
    {
        $$ = $3;
        $$->agg_type = AggFuncType::COUNT;
    }
    |   COUNT '(' '*' ')'
    {
        $$ = new Col("", "*", AggFuncType::COUNT);
    }
    ;

colList:
        col
    {
        $$ = new std::vector<Col>(); // 使用 move
        $$->emplace_back(std::move(*$1));
        delete $1;
    }
    |   colList ',' col
    {
        $$ = $1;
        $$->emplace_back(std::move(*$3)); // 使用 move
        delete $3;
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
        $$ = $1;
    }
    |   col
    {
        $$ = $1;
    }
    ;

setClauses:
        setClause
    {
        $$ = new std::vector<SetClause>(); // 使用 move
        $$->emplace_back(std::move(*$1));
        delete $1;
    }
    |   setClauses ',' setClause
    {
        $$ = $1;
        $$->emplace_back(std::move(*$3)); // 使用 move
        delete $3;
    }
    ;

setClause:
        colName '=' value
    {
        $$ = new SetClause(*$1, $3, UpdateOp::ASSINGMENT);
        delete $1;
    }
    |   colName '=' colName value
    {
        $$ = new SetClause(*$1, $4, UpdateOp::SELF_ADD);
        delete $1;
        delete $3;
    }
    |   colName '=' colName '+' value
    {
        $$ = new SetClause(*$1, $5, UpdateOp::SELF_ADD);
        delete $1;
        delete $3;
    }
    |   colName '=' colName '-' value
    {
        $$ = new SetClause(*$1, $5, UpdateOp::SELF_SUB);
        delete $1;
        delete $3;
    }
    |   colName '=' colName '*' value
    {
        $$ = new SetClause(*$1, $5, UpdateOp::SELF_MUT);
        delete $1;
        delete $3;
    }
    |   colName '=' colName DIV value
    {
        $$ = new SetClause(*$1, $5, UpdateOp::SELF_DIV);
        delete $1;
        delete $3;
    }
    ;

selector:
        '*'
    {
        $$ = new std::vector<Col>();
    }
    |   colList
    ;

tableList:
        tbName
    {
        $$ = new TableList;
        $$->tables.emplace_back(std::move(*$1)); // 使用 move
        $$->aliases.emplace_back("");
        delete $1;
    }
    |   tbName ALIAS
    {
        $$ = new TableList;
        $$->tables.emplace_back(std::move(*$1)); // 使用 move
        $$->aliases.emplace_back(std::move(*$2)); // 使用 move
        delete $1;
        delete $2;
    }
    |   tableList ',' tbName
    {
        $$ = $1;
        $$->tables.emplace_back(std::move(*$3)); // 使用 move
        $$->aliases.emplace_back("");
        delete $3;
    }
    |   tableList ',' tbName ALIAS
    {
        $$ = $1;
        $$->tables.emplace_back(std::move(*$3)); // 使用 move
        $$->aliases.emplace_back(std::move(*$4)); // 使用 move
        delete $3;
        delete $4;
    }
    |   tableList JOIN tbName optJoinClause
    {
        $$ = $1;

        $$->jointree.emplace_back($1->tables.back(), *$3, INNER_JOIN);
        $$->tables.emplace_back(std::move(*$3));
        $$->aliases.emplace_back("");
        delete $3;
        if($4 != nullptr) {
            $$->jointree.back().conds = std::move(*$4);
            delete $4;
        }
    }
    |   tableList JOIN tbName ALIAS optJoinClause
    {
        $$ = $1;

        $$->jointree.emplace_back($1->tables.back(), *$3, INNER_JOIN);
        $$->tables.emplace_back(std::move(*$3));
        $$->aliases.emplace_back(std::move(*$4));
        delete $3;
        delete $4;
        if($5 != nullptr) {
            $$->jointree.back().conds = std::move(*$5);
            delete $5;
        }
    }
    |   tableList SEMI JOIN tbName optJoinClause
    {
        $$ = $1;

        $$->jointree.emplace_back($1->tables.back(), *$4, SEMI_JOIN);
        $$->tables.emplace_back(std::move(*$4));
        $$->aliases.emplace_back("");
        delete $4;
        if($5 != nullptr) {
            $$->jointree.back().conds = std::move(*$5);
            delete $5;
        }
    }
    |   tableList SEMI JOIN tbName ALIAS optJoinClause
    {
        $$ = $1;

        $$->jointree.emplace_back($1->tables.back(), *$4, SEMI_JOIN);
        $$->tables.emplace_back(std::move(*$4));
        $$->aliases.emplace_back(std::move(*$5));
        delete $4;
        delete $5;
        if($6 != nullptr) {
            $$->jointree.back().conds = std::move(*$6);
            delete $6;
        }
    }
    ;

opt_order_clause:
    ORDER BY order_clause
    {
        $$ = $3;
    }
    |   /* epsilon */ { $$=nullptr; }
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
    |   /* epsilon */ { $$=nullptr; }
    ;

order_clause:
      order_item
    {
        $$ = new OrderBy($1->first, $1->second);
        delete $1;
    }
    |   order_clause ',' order_item
    {
        $1->addItem($3->first, $3->second);
        $$ = $1;  // 使用 move
        delete $3;
    }
    ;

order_item:
      col opt_asc_desc
    {
        $$ = new std::pair<Col, OrderByDir>(std::move(*$1), $2);
        delete $1;
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

tbName: IDENTIFIER { $$ = $1; };
colName: IDENTIFIER { $$ = $1; };
ALIAS: IDENTIFIER { $$ = $1; };
fileName: VALUE_PATH { $$ = $1; };

%%