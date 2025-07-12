    /* keywords are case insensitive */
%option caseless
    /* we don't need yywrap() function */
%option noyywrap
    /* we don't need yyunput() function */
%option nounput
    /* we don't need input() function */
%option noinput
    /* enable location */
%option bison-bridge
%option bison-locations
%option 8bit
%option full
%option nodefault

%{
#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <cmath>
#include <sstream>
#include <iomanip>

// automatically update location
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for (int i = 0; yytext[i] != '\0'; i++) { \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 1; \
        } else { \
            yylloc->last_column++; \
        } \
    }

// 浮点数精度常量
const int FLOAT_PRECISION = 6;
const float FLOAT_PRECISION_MULTIPLIER = std::pow(10, FLOAT_PRECISION);

%}

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.({digit}+)?
value_string '[^']*'
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."
value_path [\.|\/][^ \t]+\.csv

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { /* ignore new line */ }
    /* keywords */
"SHOW" { return SHOW; }
"EXPLAIN" { return EXPLAIN; }
"BEGIN" { return TXN_BEGIN; }
"COMMIT" { return TXN_COMMIT; }
"ABORT" { return TXN_ABORT; }
"ROLLBACK" { return TXN_ROLLBACK; }
"TABLES" { return TABLES; }
"CREATE" { return CREATE; }
"TABLE" { return TABLE; }
"DROP" { return DROP; }
"DESC" { return DESC; }
"INSERT" { return INSERT; }
"INTO" { return INTO; }
"VALUES" { return VALUES; }
"DELETE" { return DELETE; }
"FROM" { return FROM; }
"WHERE" { return WHERE; }
"UPDATE" { return UPDATE; }
"SET" { return SET; }
"SELECT" { return SELECT; }
"INT" { return INT; }
"CHAR" { return CHAR; }
"FLOAT" { return FLOAT; }
"DATETIME" {return DATETIME; }
"INDEX" { return INDEX; }
"AND" { return AND; }
"SEMI" { return SEMI; }
"JOIN" {return JOIN;}
"ON" { return ON; }
"IN" { return IN; }
"NOT" { return NOT; }
"EXIT" { return EXIT; }
"HELP" { return HELP; }
"ORDER" { return ORDER; }
"GROUP" { return GROUP; }
"BY" {  return BY;  }
"HAVING" { return HAVING; }
"ASC" { return ASC; }
"LIMIT" { return LIMIT; }
"SUM" { return SUM; }
"COUNT" { return COUNT; }
"MAX" { return MAX; }
"MIN" { return MIN; }
"AVG" { return AVG; }
"AS" { return AS; }
"STATIC_CHECKPOINT" { return STATIC_CHECKPOINT; }
"ENABLE_NESTLOOP" { return ENABLE_NESTLOOP; }
"ENABLE_SORTMERGE" { return ENABLE_SORTMERGE; }
"LOAD" { return LOAD; }
"OUTPUT_FILE" {return OUTPUT_FILE;}
"OFF" {return OFF;}
"TRUE" {
    yylval->sv_bool = true;
    return VALUE_BOOL;
}
"FALSE" {
    yylval->sv_bool = false;
    return VALUE_BOOL;
}
    /* operators */
">=" { return GEQ; }
"<=" { return LEQ; }
"<>" { return NEQ; }
{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->sv_str = new std::string(yytext);
    return IDENTIFIER;
}
    /* literals */
{value_int} {
    char* end;
    yylval->sv_int = strtol(yytext, &end, 10);
    return VALUE_INT;
}
{value_float} {
    double raw_val = atof(yytext);
    // 对浮点数进行精度处理
    yylval->sv_float = std::round(raw_val * FLOAT_PRECISION_MULTIPLIER) / FLOAT_PRECISION_MULTIPLIER;
    return VALUE_FLOAT;
}
{value_string} {
    yylval->sv_str = new std::string(yytext + 1, yyleng - 2);
    return VALUE_STRING;
}
{value_path} {
    yylval->sv_str = new std::string(yytext);
    return VALUE_PATH;
}
"\/" { return DIV; }

    /* EOF */
<<EOF>> { return T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%