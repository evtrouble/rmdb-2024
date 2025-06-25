/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "/home/nero/diff/db2025/src/parser/yacc.y"

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

#line 103 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "yacc.tab.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SHOW = 3,                       /* SHOW  */
  YYSYMBOL_TABLES = 4,                     /* TABLES  */
  YYSYMBOL_CREATE = 5,                     /* CREATE  */
  YYSYMBOL_TABLE = 6,                      /* TABLE  */
  YYSYMBOL_DROP = 7,                       /* DROP  */
  YYSYMBOL_DESC = 8,                       /* DESC  */
  YYSYMBOL_INSERT = 9,                     /* INSERT  */
  YYSYMBOL_INTO = 10,                      /* INTO  */
  YYSYMBOL_VALUES = 11,                    /* VALUES  */
  YYSYMBOL_DELETE = 12,                    /* DELETE  */
  YYSYMBOL_FROM = 13,                      /* FROM  */
  YYSYMBOL_ASC = 14,                       /* ASC  */
  YYSYMBOL_ORDER = 15,                     /* ORDER  */
  YYSYMBOL_GROUP = 16,                     /* GROUP  */
  YYSYMBOL_BY = 17,                        /* BY  */
  YYSYMBOL_HAVING = 18,                    /* HAVING  */
  YYSYMBOL_LIMIT = 19,                     /* LIMIT  */
  YYSYMBOL_WHERE = 20,                     /* WHERE  */
  YYSYMBOL_UPDATE = 21,                    /* UPDATE  */
  YYSYMBOL_SET = 22,                       /* SET  */
  YYSYMBOL_SELECT = 23,                    /* SELECT  */
  YYSYMBOL_INT = 24,                       /* INT  */
  YYSYMBOL_CHAR = 25,                      /* CHAR  */
  YYSYMBOL_FLOAT = 26,                     /* FLOAT  */
  YYSYMBOL_DATETIME = 27,                  /* DATETIME  */
  YYSYMBOL_INDEX = 28,                     /* INDEX  */
  YYSYMBOL_AND = 29,                       /* AND  */
  YYSYMBOL_SEMI = 30,                      /* SEMI  */
  YYSYMBOL_JOIN = 31,                      /* JOIN  */
  YYSYMBOL_ON = 32,                        /* ON  */
  YYSYMBOL_IN = 33,                        /* IN  */
  YYSYMBOL_NOT = 34,                       /* NOT  */
  YYSYMBOL_EXIT = 35,                      /* EXIT  */
  YYSYMBOL_HELP = 36,                      /* HELP  */
  YYSYMBOL_DIV = 37,                       /* DIV  */
  YYSYMBOL_EXPLAIN = 38,                   /* EXPLAIN  */
  YYSYMBOL_TXN_BEGIN = 39,                 /* TXN_BEGIN  */
  YYSYMBOL_TXN_COMMIT = 40,                /* TXN_COMMIT  */
  YYSYMBOL_TXN_ABORT = 41,                 /* TXN_ABORT  */
  YYSYMBOL_TXN_ROLLBACK = 42,              /* TXN_ROLLBACK  */
  YYSYMBOL_ORDER_BY = 43,                  /* ORDER_BY  */
  YYSYMBOL_ENABLE_NESTLOOP = 44,           /* ENABLE_NESTLOOP  */
  YYSYMBOL_ENABLE_SORTMERGE = 45,          /* ENABLE_SORTMERGE  */
  YYSYMBOL_STATIC_CHECKPOINT = 46,         /* STATIC_CHECKPOINT  */
  YYSYMBOL_SUM = 47,                       /* SUM  */
  YYSYMBOL_COUNT = 48,                     /* COUNT  */
  YYSYMBOL_MAX = 49,                       /* MAX  */
  YYSYMBOL_MIN = 50,                       /* MIN  */
  YYSYMBOL_AVG = 51,                       /* AVG  */
  YYSYMBOL_AS = 52,                        /* AS  */
  YYSYMBOL_LEQ = 53,                       /* LEQ  */
  YYSYMBOL_NEQ = 54,                       /* NEQ  */
  YYSYMBOL_GEQ = 55,                       /* GEQ  */
  YYSYMBOL_T_EOF = 56,                     /* T_EOF  */
  YYSYMBOL_IDENTIFIER = 57,                /* IDENTIFIER  */
  YYSYMBOL_VALUE_STRING = 58,              /* VALUE_STRING  */
  YYSYMBOL_VALUE_PATH = 59,                /* VALUE_PATH  */
  YYSYMBOL_VALUE_INT = 60,                 /* VALUE_INT  */
  YYSYMBOL_VALUE_FLOAT = 61,               /* VALUE_FLOAT  */
  YYSYMBOL_VALUE_BOOL = 62,                /* VALUE_BOOL  */
  YYSYMBOL_63_ = 63,                       /* ';'  */
  YYSYMBOL_64_ = 64,                       /* '='  */
  YYSYMBOL_65_ = 65,                       /* '('  */
  YYSYMBOL_66_ = 66,                       /* ')'  */
  YYSYMBOL_67_ = 67,                       /* ','  */
  YYSYMBOL_68_ = 68,                       /* '.'  */
  YYSYMBOL_69_ = 69,                       /* '*'  */
  YYSYMBOL_70_ = 70,                       /* '<'  */
  YYSYMBOL_71_ = 71,                       /* '>'  */
  YYSYMBOL_72_ = 72,                       /* '+'  */
  YYSYMBOL_73_ = 73,                       /* '-'  */
  YYSYMBOL_YYACCEPT = 74,                  /* $accept  */
  YYSYMBOL_start = 75,                     /* start  */
  YYSYMBOL_stmt = 76,                      /* stmt  */
  YYSYMBOL_txnStmt = 77,                   /* txnStmt  */
  YYSYMBOL_dbStmt = 78,                    /* dbStmt  */
  YYSYMBOL_setStmt = 79,                   /* setStmt  */
  YYSYMBOL_ddl = 80,                       /* ddl  */
  YYSYMBOL_dml = 81,                       /* dml  */
  YYSYMBOL_fieldList = 82,                 /* fieldList  */
  YYSYMBOL_colNameList = 83,               /* colNameList  */
  YYSYMBOL_field = 84,                     /* field  */
  YYSYMBOL_type = 85,                      /* type  */
  YYSYMBOL_valueList = 86,                 /* valueList  */
  YYSYMBOL_value = 87,                     /* value  */
  YYSYMBOL_condition = 88,                 /* condition  */
  YYSYMBOL_optWhereClause = 89,            /* optWhereClause  */
  YYSYMBOL_optJoinClause = 90,             /* optJoinClause  */
  YYSYMBOL_opt_having_clause = 91,         /* opt_having_clause  */
  YYSYMBOL_whereClause = 92,               /* whereClause  */
  YYSYMBOL_col = 93,                       /* col  */
  YYSYMBOL_aggCol = 94,                    /* aggCol  */
  YYSYMBOL_colList = 95,                   /* colList  */
  YYSYMBOL_op = 96,                        /* op  */
  YYSYMBOL_expr = 97,                      /* expr  */
  YYSYMBOL_setClauses = 98,                /* setClauses  */
  YYSYMBOL_setClause = 99,                 /* setClause  */
  YYSYMBOL_selector = 100,                 /* selector  */
  YYSYMBOL_tableList = 101,                /* tableList  */
  YYSYMBOL_opt_order_clause = 102,         /* opt_order_clause  */
  YYSYMBOL_opt_limit_clause = 103,         /* opt_limit_clause  */
  YYSYMBOL_opt_groupby_clause = 104,       /* opt_groupby_clause  */
  YYSYMBOL_order_clause = 105,             /* order_clause  */
  YYSYMBOL_order_item = 106,               /* order_item  */
  YYSYMBOL_opt_asc_desc = 107,             /* opt_asc_desc  */
  YYSYMBOL_set_knob_type = 108,            /* set_knob_type  */
  YYSYMBOL_tbName = 109,                   /* tbName  */
  YYSYMBOL_colName = 110,                  /* colName  */
  YYSYMBOL_ALIAS = 111                     /* ALIAS  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
             && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  54
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   218

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  74
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  38
/* YYNRULES -- Number of rules.  */
#define YYNRULES  110
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  209

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   317


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      65,    66,    69,    72,    67,    73,    68,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    63,
      70,    64,    71,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    79,    79,    84,    89,    94,   102,   103,   104,   105,
     106,   107,   114,   118,   122,   126,   133,   140,   147,   151,
     155,   159,   163,   167,   171,   178,   182,   186,   190,   207,
     211,   218,   222,   229,   236,   240,   244,   248,   255,   259,
     266,   270,   275,   279,   286,   294,   295,   302,   303,   310,
     311,   318,   322,   330,   334,   339,   343,   348,   356,   360,
     364,   368,   372,   376,   383,   387,   394,   398,   402,   406,
     410,   414,   418,   422,   429,   433,   440,   444,   451,   455,
     459,   463,   467,   471,   478,   482,   486,   492,   498,   506,
     514,   529,   544,   559,   577,   581,   585,   590,   596,   600,
     604,   608,   616,   623,   624,   625,   629,   630,   633,   635,
     637
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "SHOW", "TABLES",
  "CREATE", "TABLE", "DROP", "DESC", "INSERT", "INTO", "VALUES", "DELETE",
  "FROM", "ASC", "ORDER", "GROUP", "BY", "HAVING", "LIMIT", "WHERE",
  "UPDATE", "SET", "SELECT", "INT", "CHAR", "FLOAT", "DATETIME", "INDEX",
  "AND", "SEMI", "JOIN", "ON", "IN", "NOT", "EXIT", "HELP", "DIV",
  "EXPLAIN", "TXN_BEGIN", "TXN_COMMIT", "TXN_ABORT", "TXN_ROLLBACK",
  "ORDER_BY", "ENABLE_NESTLOOP", "ENABLE_SORTMERGE", "STATIC_CHECKPOINT",
  "SUM", "COUNT", "MAX", "MIN", "AVG", "AS", "LEQ", "NEQ", "GEQ", "T_EOF",
  "IDENTIFIER", "VALUE_STRING", "VALUE_PATH", "VALUE_INT", "VALUE_FLOAT",
  "VALUE_BOOL", "';'", "'='", "'('", "')'", "','", "'.'", "'*'", "'<'",
  "'>'", "'+'", "'-'", "$accept", "start", "stmt", "txnStmt", "dbStmt",
  "setStmt", "ddl", "dml", "fieldList", "colNameList", "field", "type",
  "valueList", "value", "condition", "optWhereClause", "optJoinClause",
  "opt_having_clause", "whereClause", "col", "aggCol", "colList", "op",
  "expr", "setClauses", "setClause", "selector", "tableList",
  "opt_order_clause", "opt_limit_clause", "opt_groupby_clause",
  "order_clause", "order_item", "opt_asc_desc", "set_knob_type", "tbName",
  "colName", "ALIAS", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-142)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-109)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      69,     7,     1,     8,   -51,     3,     9,   -51,   -24,    72,
    -142,  -142,   137,  -142,  -142,  -142,  -142,  -142,    46,   -13,
    -142,  -142,  -142,  -142,  -142,  -142,    35,   -51,   -51,  -142,
     -51,   -51,  -142,  -142,   -51,   -51,    39,  -142,  -142,     4,
      -2,    10,    14,    17,    20,     5,  -142,  -142,    43,    34,
      90,    48,    74,  -142,  -142,  -142,   -51,    67,    73,  -142,
      77,   128,   114,    86,    83,    49,   104,    49,    49,    49,
      99,    49,   -51,    86,    99,  -142,    86,    86,    86,    92,
      49,  -142,  -142,   -15,  -142,    95,  -142,    96,    97,   105,
     106,   110,   116,  -142,  -142,  -142,   -12,    99,  -142,  -142,
     -26,  -142,   168,   -22,  -142,    -1,   129,  -142,   141,    60,
      86,  -142,   123,  -142,  -142,  -142,  -142,  -142,  -142,   155,
     -51,   -51,   180,  -142,  -142,    86,  -142,   132,  -142,  -142,
    -142,  -142,    86,  -142,  -142,  -142,  -142,  -142,    61,  -142,
      49,  -142,   166,  -142,  -142,  -142,  -142,  -142,  -142,   117,
    -142,  -142,    75,   -51,   -23,    99,   183,   184,  -142,   143,
    -142,  -142,   129,  -142,  -142,  -142,  -142,  -142,   129,   129,
     129,   129,  -142,   -23,    49,  -142,   169,  -142,    49,    49,
     189,   139,  -142,  -142,  -142,  -142,  -142,  -142,   169,   141,
    -142,    34,   141,   190,   187,  -142,  -142,    49,   148,  -142,
      29,   142,  -142,  -142,  -142,  -142,  -142,    49,  -142
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       4,     3,     0,    12,    13,    14,    15,     5,     0,     0,
       9,     6,    10,     7,     8,    16,     0,     0,     0,    24,
       0,     0,   108,    20,     0,     0,     0,   106,   107,     0,
       0,     0,     0,     0,     0,   109,    84,    64,    55,    85,
       0,     0,    54,    11,     1,     2,     0,     0,     0,    19,
       0,     0,    45,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    23,     0,     0,     0,     0,
       0,    26,   109,    45,    76,     0,    17,     0,     0,     0,
       0,     0,     0,   110,    57,    65,    45,    86,    53,    56,
       0,    29,     0,     0,    31,     0,     0,    51,    46,     0,
       0,    27,     0,    58,    63,    62,    60,    59,    61,     0,
       0,     0,    99,    87,    18,     0,    34,     0,    36,    37,
      33,    21,     0,    22,    42,    40,    41,    43,     0,    38,
       0,    72,     0,    70,    69,    71,    66,    67,    68,     0,
      77,    78,     0,     0,    47,    88,     0,    49,    30,     0,
      32,    25,     0,    52,    73,    74,    75,    44,     0,     0,
       0,     0,    79,    47,     0,    90,    47,    89,     0,     0,
      95,     0,    39,    83,    82,    80,    81,    92,    47,    48,
      91,    98,    50,     0,    97,    35,    93,     0,     0,    28,
     105,    94,   100,    96,   104,   103,   102,     0,   101
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -142,  -142,  -142,  -142,  -142,  -142,  -142,   198,  -142,   133,
      87,  -142,  -142,   -82,    76,   -73,  -134,  -142,  -141,    -9,
    -142,    36,  -142,  -142,  -142,   103,  -142,  -142,  -142,  -142,
    -142,  -142,    11,  -142,  -142,    -3,   -61,   -71
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    18,    19,    20,    21,    22,    23,    24,   100,   103,
     101,   130,   138,   139,   107,    81,   175,   180,   108,   109,
      48,    49,   149,   167,    83,    84,    50,    96,   194,   199,
     157,   201,   202,   206,    39,    51,    52,    94
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      47,    33,    85,    99,    36,    80,    32,    27,    80,   174,
     111,    25,    98,    34,    30,   102,   104,   104,   119,   120,
      37,    38,    35,   122,    57,    58,   123,    59,    60,    28,
     151,    61,    62,   189,    93,    26,    31,   204,   192,   187,
     124,   125,   190,   205,   131,   132,    54,    29,    56,    85,
      55,   152,   110,    75,   196,   121,    87,    89,    90,    91,
      92,    63,    95,    65,   102,   133,   132,   165,    64,    97,
     172,   160,     1,  -108,     2,    66,     3,     4,     5,    67,
     182,     6,    68,   176,   177,    69,   183,   184,   185,   186,
       7,     8,     9,   141,   142,    70,    40,    41,    42,    43,
      44,    71,   188,    72,    10,    11,    45,    12,    13,    14,
      15,    16,   168,   143,   144,   145,    73,   154,   155,    40,
      41,    42,    43,    44,   146,    17,    74,   161,   162,    45,
     147,   148,    76,   134,    80,   135,   136,   137,    77,    79,
     166,    46,    78,    82,   169,    86,     5,   170,   171,     6,
     173,    40,    41,    42,    43,    44,    93,   106,     7,   112,
       9,    45,   113,   114,    40,    41,    42,    43,    44,    47,
     140,   115,   116,    88,    45,   134,   117,   135,   136,   137,
      82,   134,   118,   135,   136,   137,   153,   134,   200,   135,
     136,   137,   126,   127,   128,   129,   156,   159,   200,   164,
     178,   174,   179,   181,   193,   195,   198,   197,   203,   207,
      53,   105,   158,   150,   191,     0,   163,     0,   208
};

static const yytype_int16 yycheck[] =
{
       9,     4,    63,    74,     7,    20,    57,     6,    20,    32,
      83,     4,    73,    10,     6,    76,    77,    78,    30,    31,
      44,    45,    13,    96,    27,    28,    97,    30,    31,    28,
     112,    34,    35,   174,    57,    28,    28,     8,   179,   173,
      66,    67,   176,    14,    66,    67,     0,    46,    13,   110,
      63,   112,    67,    56,   188,    67,    65,    66,    67,    68,
      69,    22,    71,    65,   125,    66,    67,   149,    64,    72,
     152,   132,     3,    68,     5,    65,     7,     8,     9,    65,
     162,    12,    65,   154,   155,    65,   168,   169,   170,   171,
      21,    22,    23,    33,    34,    52,    47,    48,    49,    50,
      51,    67,   173,    13,    35,    36,    57,    38,    39,    40,
      41,    42,    37,    53,    54,    55,    68,   120,   121,    47,
      48,    49,    50,    51,    64,    56,    52,    66,    67,    57,
      70,    71,    65,    58,    20,    60,    61,    62,    65,    11,
     149,    69,    65,    57,    69,    62,     9,    72,    73,    12,
     153,    47,    48,    49,    50,    51,    57,    65,    21,    64,
      23,    57,    66,    66,    47,    48,    49,    50,    51,   178,
      29,    66,    66,    69,    57,    58,    66,    60,    61,    62,
      57,    58,    66,    60,    61,    62,    31,    58,   197,    60,
      61,    62,    24,    25,    26,    27,    16,    65,   207,    33,
      17,    32,    18,    60,    15,    66,    19,    17,    60,    67,
      12,    78,   125,   110,   178,    -1,   140,    -1,   207
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     5,     7,     8,     9,    12,    21,    22,    23,
      35,    36,    38,    39,    40,    41,    42,    56,    75,    76,
      77,    78,    79,    80,    81,     4,    28,     6,    28,    46,
       6,    28,    57,   109,    10,    13,   109,    44,    45,   108,
      47,    48,    49,    50,    51,    57,    69,    93,    94,    95,
     100,   109,   110,    81,     0,    63,    13,   109,   109,   109,
     109,   109,   109,    22,    64,    65,    65,    65,    65,    65,
      52,    67,    13,    68,    52,   109,    65,    65,    65,    11,
      20,    89,    57,    98,    99,   110,    62,    93,    69,    93,
      93,    93,    93,    57,   111,    93,   101,   109,   110,   111,
      82,    84,   110,    83,   110,    83,    65,    88,    92,    93,
      67,    89,    64,    66,    66,    66,    66,    66,    66,    30,
      31,    67,    89,   111,    66,    67,    24,    25,    26,    27,
      85,    66,    67,    66,    58,    60,    61,    62,    86,    87,
      29,    33,    34,    53,    54,    55,    64,    70,    71,    96,
      99,    87,   110,    31,   109,   109,    16,   104,    84,    65,
     110,    66,    67,    88,    33,    87,    93,    97,    37,    69,
      72,    73,    87,   109,    32,    90,   111,   111,    17,    18,
      91,    60,    87,    87,    87,    87,    87,    90,   111,    92,
      90,    95,    92,    15,   102,    66,    90,    17,    19,   103,
      93,   105,   106,    60,     8,    14,   107,    67,   106
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    74,    75,    75,    75,    75,    76,    76,    76,    76,
      76,    76,    77,    77,    77,    77,    78,    79,    80,    80,
      80,    80,    80,    80,    80,    81,    81,    81,    81,    82,
      82,    83,    83,    84,    85,    85,    85,    85,    86,    86,
      87,    87,    87,    87,    88,    89,    89,    90,    90,    91,
      91,    92,    92,    93,    93,    93,    93,    93,    94,    94,
      94,    94,    94,    94,    95,    95,    96,    96,    96,    96,
      96,    96,    96,    96,    97,    97,    98,    98,    99,    99,
      99,    99,    99,    99,   100,   100,   101,   101,   101,   101,
     101,   101,   101,   101,   102,   102,   103,   103,   104,   104,
     105,   105,   106,   107,   107,   107,   108,   108,   109,   110,
     111
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     2,     1,     1,     1,     1,     2,     4,     6,     3,
       2,     6,     6,     4,     2,     7,     4,     5,     9,     1,
       3,     1,     3,     2,     1,     4,     1,     1,     1,     3,
       1,     1,     1,     1,     3,     0,     2,     0,     2,     0,
       2,     1,     3,     3,     1,     1,     3,     3,     4,     4,
       4,     4,     4,     4,     1,     3,     1,     1,     1,     1,
       1,     1,     1,     2,     1,     1,     1,     3,     3,     4,
       5,     5,     5,     5,     1,     1,     1,     2,     3,     4,
       4,     5,     5,     6,     3,     0,     2,     0,     3,     0,
       1,     3,     2,     1,     1,     0,     1,     1,     1,     1,
       1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YYLOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YYLOCATION_PRINT

#  if defined YY_LOCATION_PRINT

   /* Temporary convenience wrapper in case some people defined the
      undocumented and private YY_LOCATION_PRINT macros.  */
#   define YYLOCATION_PRINT(File, Loc)  YY_LOCATION_PRINT(File, *(Loc))

#  elif defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
}

#   define YYLOCATION_PRINT  yy_location_print_

    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT(File, Loc)  YYLOCATION_PRINT(File, &(Loc))

#  else

#   define YYLOCATION_PRINT(File, Loc) ((void) 0)
    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT  YYLOCATION_PRINT

#  endif
# endif /* !defined YYLOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YYLOCATION_PRINT (yyo, yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]));
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* start: stmt ';'  */
#line 80 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        parse_tree = (yyvsp[-1].sv_node);
        YYACCEPT;
    }
#line 1748 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 3: /* start: HELP  */
#line 85 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
#line 1757 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 4: /* start: EXIT  */
#line 90 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1766 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 5: /* start: T_EOF  */
#line 95 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1775 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 11: /* stmt: EXPLAIN dml  */
#line 108 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ExplainStmt>((yyvsp[0].sv_node));
    }
#line 1783 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 12: /* txnStmt: TXN_BEGIN  */
#line 115 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnBegin>();
    }
#line 1791 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 13: /* txnStmt: TXN_COMMIT  */
#line 119 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnCommit>();
    }
#line 1799 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 14: /* txnStmt: TXN_ABORT  */
#line 123 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnAbort>();
    }
#line 1807 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 15: /* txnStmt: TXN_ROLLBACK  */
#line 127 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnRollback>();
    }
#line 1815 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 16: /* dbStmt: SHOW TABLES  */
#line 134 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ShowTables>();
    }
#line 1823 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 17: /* setStmt: SET set_knob_type '=' VALUE_BOOL  */
#line 141 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<SetStmt>((yyvsp[-2].sv_setKnobType), (yyvsp[0].sv_bool));
    }
#line 1831 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 18: /* ddl: CREATE TABLE tbName '(' fieldList ')'  */
#line 148 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateTable>((yyvsp[-3].sv_str), (yyvsp[-1].sv_fields));
    }
#line 1839 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 19: /* ddl: DROP TABLE tbName  */
#line 152 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DropTable>((yyvsp[0].sv_str));
    }
#line 1847 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 20: /* ddl: DESC tbName  */
#line 156 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DescTable>((yyvsp[0].sv_str));
    }
#line 1855 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 21: /* ddl: CREATE INDEX tbName '(' colNameList ')'  */
#line 160 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateIndex>((yyvsp[-3].sv_str), (yyvsp[-1].sv_strs));
    }
#line 1863 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 22: /* ddl: DROP INDEX tbName '(' colNameList ')'  */
#line 164 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DropIndex>((yyvsp[-3].sv_str), (yyvsp[-1].sv_strs));
    }
#line 1871 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 23: /* ddl: SHOW INDEX FROM tbName  */
#line 168 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ShowIndex>((yyvsp[0].sv_str));
    }
#line 1879 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 24: /* ddl: CREATE STATIC_CHECKPOINT  */
#line 172 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateStaticCheckpoint>();
    }
#line 1887 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 25: /* dml: INSERT INTO tbName VALUES '(' valueList ')'  */
#line 179 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<InsertStmt>((yyvsp[-4].sv_str), (yyvsp[-1].sv_vals));
    }
#line 1895 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 26: /* dml: DELETE FROM tbName optWhereClause  */
#line 183 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DeleteStmt>((yyvsp[-1].sv_str), (yyvsp[0].sv_conds));
    }
#line 1903 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 27: /* dml: UPDATE tbName SET setClauses optWhereClause  */
#line 187 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<UpdateStmt>((yyvsp[-3].sv_str), (yyvsp[-1].sv_set_clauses), (yyvsp[0].sv_conds));
    }
#line 1911 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 28: /* dml: SELECT selector FROM tableList optWhereClause opt_groupby_clause opt_having_clause opt_order_clause opt_limit_clause  */
#line 191 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<SelectStmt>(
            (yyvsp[-7].sv_cols),             // selector
            (yyvsp[-5].sv_table_list).tables,      // 表列表
            (yyvsp[-5].sv_table_list).jointree,    // 连接树
            (yyvsp[-4].sv_conds),             // where条件
            (yyvsp[-3].sv_cols),             // groupby
            (yyvsp[-2].sv_conds),             // having
            (yyvsp[-1].sv_orderby),             // order
            (yyvsp[0].sv_int),             // limit
            (yyvsp[-5].sv_table_list).aliases      // 表别名
        );
    }
#line 1929 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 29: /* fieldList: field  */
#line 208 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_fields) = std::vector<std::shared_ptr<Field>>{(yyvsp[0].sv_field)};
    }
#line 1937 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 30: /* fieldList: fieldList ',' field  */
#line 212 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_fields).emplace_back((yyvsp[0].sv_field));
    }
#line 1945 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 31: /* colNameList: colName  */
#line 219 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_strs) = std::vector<std::string>{(yyvsp[0].sv_str)};
    }
#line 1953 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 32: /* colNameList: colNameList ',' colName  */
#line 223 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_strs).emplace_back((yyvsp[0].sv_str));
    }
#line 1961 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 33: /* field: colName type  */
#line 230 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_field) = std::make_shared<ColDef>((yyvsp[-1].sv_str), (yyvsp[0].sv_type_len));
    }
#line 1969 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 34: /* type: INT  */
#line 237 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
#line 1977 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 35: /* type: CHAR '(' VALUE_INT ')'  */
#line 241 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_STRING, (yyvsp[-1].sv_int));
    }
#line 1985 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 36: /* type: FLOAT  */
#line 245 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
#line 1993 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 37: /* type: DATETIME  */
#line 249 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_DATETIME, 19);
    }
#line 2001 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 38: /* valueList: value  */
#line 256 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_vals) = std::vector<std::shared_ptr<Value>>{(yyvsp[0].sv_val)};
    }
#line 2009 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 39: /* valueList: valueList ',' value  */
#line 260 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_vals).emplace_back((yyvsp[0].sv_val));
    }
#line 2017 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 40: /* value: VALUE_INT  */
#line 267 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<IntLit>((yyvsp[0].sv_int));
    }
#line 2025 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 41: /* value: VALUE_FLOAT  */
#line 271 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        // 浮点数在词法分析阶段已经进行了精度处理
        (yyval.sv_val) = std::make_shared<FloatLit>((yyvsp[0].sv_float));
    }
#line 2034 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 42: /* value: VALUE_STRING  */
#line 276 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<StringLit>((yyvsp[0].sv_str));
    }
#line 2042 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 43: /* value: VALUE_BOOL  */
#line 280 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<BoolLit>((yyvsp[0].sv_bool));
    }
#line 2050 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 44: /* condition: col op expr  */
#line 287 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cond) = std::make_shared<BinaryExpr>((yyvsp[-2].sv_col), (yyvsp[-1].sv_comp_op), (yyvsp[0].sv_expr));
    }
#line 2058 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 45: /* optWhereClause: %empty  */
#line 294 "/home/nero/diff/db2025/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2064 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 46: /* optWhereClause: WHERE whereClause  */
#line 296 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2072 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 47: /* optJoinClause: %empty  */
#line 302 "/home/nero/diff/db2025/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2078 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 48: /* optJoinClause: ON whereClause  */
#line 304 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2086 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 49: /* opt_having_clause: %empty  */
#line 310 "/home/nero/diff/db2025/src/parser/yacc.y"
                  { /* ignore*/ }
#line 2092 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 50: /* opt_having_clause: HAVING whereClause  */
#line 312 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2100 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 51: /* whereClause: condition  */
#line 319 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = std::vector<std::shared_ptr<BinaryExpr>>{(yyvsp[0].sv_cond)};
    }
#line 2108 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 52: /* whereClause: whereClause AND condition  */
#line 323 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds).emplace_back((yyvsp[0].sv_cond));
    }
#line 2116 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 53: /* col: tbName '.' colName  */
#line 331 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-2].sv_str), (yyvsp[0].sv_str));
    }
#line 2124 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 54: /* col: colName  */
#line 335 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[0].sv_str));
    }
#line 2132 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 55: /* col: aggCol  */
#line 340 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[0].sv_col);
    }
#line 2140 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 56: /* col: colName AS ALIAS  */
#line 344 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[-2].sv_str));
        (yyval.sv_col)->alias = (yyvsp[0].sv_str);
    }
#line 2149 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 57: /* col: aggCol AS ALIAS  */
#line 349 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-2].sv_col);
        (yyval.sv_col)->alias = (yyvsp[0].sv_str);
    }
#line 2158 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 58: /* aggCol: SUM '(' col ')'  */
#line 357 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-1].sv_col)->tab_name, (yyvsp[-1].sv_col)->col_name, AggFuncType::SUM);
    }
#line 2166 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 59: /* aggCol: MIN '(' col ')'  */
#line 361 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-1].sv_col)->tab_name, (yyvsp[-1].sv_col)->col_name, AggFuncType::MIN);
    }
#line 2174 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 60: /* aggCol: MAX '(' col ')'  */
#line 365 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-1].sv_col)->tab_name, (yyvsp[-1].sv_col)->col_name, AggFuncType::MAX);
    }
#line 2182 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 61: /* aggCol: AVG '(' col ')'  */
#line 369 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-1].sv_col)->tab_name, (yyvsp[-1].sv_col)->col_name, AggFuncType::AVG);
    }
#line 2190 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 62: /* aggCol: COUNT '(' col ')'  */
#line 373 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-1].sv_col)->tab_name, (yyvsp[-1].sv_col)->col_name, AggFuncType::COUNT);
    }
#line 2198 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 63: /* aggCol: COUNT '(' '*' ')'  */
#line 377 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", "*", AggFuncType::COUNT);
    }
#line 2206 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 64: /* colList: col  */
#line 384 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = std::vector<std::shared_ptr<Col>>{(yyvsp[0].sv_col)};
    }
#line 2214 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 65: /* colList: colList ',' col  */
#line 388 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols).emplace_back((yyvsp[0].sv_col));
    }
#line 2222 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 66: /* op: '='  */
#line 395 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_EQ;
    }
#line 2230 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 67: /* op: '<'  */
#line 399 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LT;
    }
#line 2238 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 68: /* op: '>'  */
#line 403 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GT;
    }
#line 2246 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 69: /* op: NEQ  */
#line 407 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_NE;
    }
#line 2254 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 70: /* op: LEQ  */
#line 411 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LE;
    }
#line 2262 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 71: /* op: GEQ  */
#line 415 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GE;
    }
#line 2270 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 72: /* op: IN  */
#line 419 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
	    (yyval.sv_comp_op) = SV_OP_IN;
    }
#line 2278 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 73: /* op: NOT IN  */
#line 423 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
    	(yyval.sv_comp_op) = SV_OP_NOT_IN;
    }
#line 2286 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 74: /* expr: value  */
#line 430 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyvsp[0].sv_val));
    }
#line 2294 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 75: /* expr: col  */
#line 434 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyvsp[0].sv_col));
    }
#line 2302 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 76: /* setClauses: setClause  */
#line 441 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses) = std::vector<std::shared_ptr<SetClause>>{(yyvsp[0].sv_set_clause)};
    }
#line 2310 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 77: /* setClauses: setClauses ',' setClause  */
#line 445 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses).emplace_back((yyvsp[0].sv_set_clause));
    }
#line 2318 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 78: /* setClause: colName '=' value  */
#line 452 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-2].sv_str), (yyvsp[0].sv_val), UpdateOp::ASSINGMENT);
    }
#line 2326 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 79: /* setClause: colName '=' colName value  */
#line 456 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-3].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_ADD);
    }
#line 2334 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 80: /* setClause: colName '=' colName '+' value  */
#line 460 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_ADD);
    }
#line 2342 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 81: /* setClause: colName '=' colName '-' value  */
#line 464 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_SUB);
    }
#line 2350 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 82: /* setClause: colName '=' colName '*' value  */
#line 468 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_MUT);
    }
#line 2358 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 83: /* setClause: colName '=' colName DIV value  */
#line 472 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_DIV);
    }
#line 2366 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 84: /* selector: '*'  */
#line 479 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = {};
    }
#line 2374 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 86: /* tableList: tbName  */
#line 487 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list).tables = {(yyvsp[0].sv_str)};
        (yyval.sv_table_list).aliases = {""};
        (yyval.sv_table_list).jointree = {};
    }
#line 2384 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 87: /* tableList: tbName ALIAS  */
#line 493 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list).tables = {(yyvsp[-1].sv_str)};
        (yyval.sv_table_list).aliases = {(yyvsp[0].sv_str)};
        (yyval.sv_table_list).jointree = {};
    }
#line 2394 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 88: /* tableList: tableList ',' tbName  */
#line 499 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list).tables = (yyvsp[-2].sv_table_list).tables;
        (yyval.sv_table_list).aliases = (yyvsp[-2].sv_table_list).aliases;
        (yyval.sv_table_list).tables.emplace_back((yyvsp[0].sv_str));
        (yyval.sv_table_list).aliases.emplace_back("");
        (yyval.sv_table_list).jointree = (yyvsp[-2].sv_table_list).jointree;
    }
#line 2406 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 89: /* tableList: tableList ',' tbName ALIAS  */
#line 507 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list).tables = (yyvsp[-3].sv_table_list).tables;
        (yyval.sv_table_list).aliases = (yyvsp[-3].sv_table_list).aliases;
        (yyval.sv_table_list).tables.emplace_back((yyvsp[-1].sv_str));
        (yyval.sv_table_list).aliases.emplace_back((yyvsp[0].sv_str));
        (yyval.sv_table_list).jointree = (yyvsp[-3].sv_table_list).jointree;
    }
#line 2418 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 90: /* tableList: tableList JOIN tbName optJoinClause  */
#line 515 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        auto join_expr = std::make_shared<JoinExpr>(
            (yyvsp[-3].sv_table_list).tables.back(),
            (yyvsp[-1].sv_str),
            (yyvsp[0].sv_conds),
            INNER_JOIN
        );
        (yyval.sv_table_list).tables = (yyvsp[-3].sv_table_list).tables;
        (yyval.sv_table_list).aliases = (yyvsp[-3].sv_table_list).aliases;
        (yyval.sv_table_list).tables.emplace_back((yyvsp[-1].sv_str));
        (yyval.sv_table_list).aliases.emplace_back("");
        (yyval.sv_table_list).jointree = (yyvsp[-3].sv_table_list).jointree;
        (yyval.sv_table_list).jointree.emplace_back(join_expr);
    }
#line 2437 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 91: /* tableList: tableList JOIN tbName ALIAS optJoinClause  */
#line 530 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        auto join_expr = std::make_shared<JoinExpr>(
            (yyvsp[-4].sv_table_list).tables.back(),
            (yyvsp[-2].sv_str),
            (yyvsp[0].sv_conds),
            INNER_JOIN
        );
        (yyval.sv_table_list).tables = (yyvsp[-4].sv_table_list).tables;
        (yyval.sv_table_list).aliases = (yyvsp[-4].sv_table_list).aliases;
        (yyval.sv_table_list).tables.emplace_back((yyvsp[-2].sv_str));
        (yyval.sv_table_list).aliases.emplace_back((yyvsp[-1].sv_str));
        (yyval.sv_table_list).jointree = (yyvsp[-4].sv_table_list).jointree;
        (yyval.sv_table_list).jointree.emplace_back(join_expr);
    }
#line 2456 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 92: /* tableList: tableList SEMI JOIN tbName optJoinClause  */
#line 545 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        auto join_expr = std::make_shared<JoinExpr>(
            (yyvsp[-4].sv_table_list).tables.back(),
            (yyvsp[-1].sv_str),
            (yyvsp[0].sv_conds),
            SEMI_JOIN
        );
        (yyval.sv_table_list).tables = (yyvsp[-4].sv_table_list).tables;
        (yyval.sv_table_list).aliases = (yyvsp[-4].sv_table_list).aliases;
        (yyval.sv_table_list).tables.emplace_back((yyvsp[-1].sv_str));
        (yyval.sv_table_list).aliases.emplace_back("");
        (yyval.sv_table_list).jointree = (yyvsp[-4].sv_table_list).jointree;
        (yyval.sv_table_list).jointree.emplace_back(join_expr);
    }
#line 2475 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 93: /* tableList: tableList SEMI JOIN tbName ALIAS optJoinClause  */
#line 560 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        auto join_expr = std::make_shared<JoinExpr>(
            (yyvsp[-5].sv_table_list).tables.back(),
            (yyvsp[-2].sv_str),
            (yyvsp[0].sv_conds),
            SEMI_JOIN
        );
        (yyval.sv_table_list).tables = (yyvsp[-5].sv_table_list).tables;
        (yyval.sv_table_list).aliases = (yyvsp[-5].sv_table_list).aliases;
        (yyval.sv_table_list).tables.emplace_back((yyvsp[-2].sv_str));
        (yyval.sv_table_list).aliases.emplace_back((yyvsp[-1].sv_str));
        (yyval.sv_table_list).jointree = (yyvsp[-5].sv_table_list).jointree;
        (yyval.sv_table_list).jointree.emplace_back(join_expr);
    }
#line 2494 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 94: /* opt_order_clause: ORDER BY order_clause  */
#line 578 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_orderby) = (yyvsp[0].sv_orderby);
    }
#line 2502 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 95: /* opt_order_clause: %empty  */
#line 581 "/home/nero/diff/db2025/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2508 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 96: /* opt_limit_clause: LIMIT VALUE_INT  */
#line 586 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_int) = (yyvsp[0].sv_int);
    }
#line 2516 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 97: /* opt_limit_clause: %empty  */
#line 590 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_int) = -1;
    }
#line 2524 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 98: /* opt_groupby_clause: GROUP BY colList  */
#line 597 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = (yyvsp[0].sv_cols);
    }
#line 2532 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 99: /* opt_groupby_clause: %empty  */
#line 600 "/home/nero/diff/db2025/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2538 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 100: /* order_clause: order_item  */
#line 605 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_orderby) = std::make_shared<OrderBy>((yyvsp[0].sv_order_item).first, (yyvsp[0].sv_order_item).second);
    }
#line 2546 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 101: /* order_clause: order_clause ',' order_item  */
#line 609 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyvsp[-2].sv_orderby)->addItem((yyvsp[0].sv_order_item).first, (yyvsp[0].sv_order_item).second);
        (yyval.sv_orderby) = (yyvsp[-2].sv_orderby);
    }
#line 2555 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 102: /* order_item: col opt_asc_desc  */
#line 617 "/home/nero/diff/db2025/src/parser/yacc.y"
    {
        (yyval.sv_order_item) = std::make_pair((yyvsp[-1].sv_col), (yyvsp[0].sv_orderby_dir));
    }
#line 2563 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 103: /* opt_asc_desc: ASC  */
#line 623 "/home/nero/diff/db2025/src/parser/yacc.y"
                 { (yyval.sv_orderby_dir) = OrderBy_ASC;     }
#line 2569 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 104: /* opt_asc_desc: DESC  */
#line 624 "/home/nero/diff/db2025/src/parser/yacc.y"
                 { (yyval.sv_orderby_dir) = OrderBy_DESC;    }
#line 2575 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 105: /* opt_asc_desc: %empty  */
#line 625 "/home/nero/diff/db2025/src/parser/yacc.y"
            { (yyval.sv_orderby_dir) = OrderBy_DEFAULT; }
#line 2581 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 106: /* set_knob_type: ENABLE_NESTLOOP  */
#line 629 "/home/nero/diff/db2025/src/parser/yacc.y"
                    { (yyval.sv_setKnobType) = ast::SetKnobType::EnableNestLoop; }
#line 2587 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;

  case 107: /* set_knob_type: ENABLE_SORTMERGE  */
#line 630 "/home/nero/diff/db2025/src/parser/yacc.y"
                         { (yyval.sv_setKnobType) = ast::SetKnobType::EnableSortMerge; }
#line 2593 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"
    break;


#line 2597 "/home/nero/diff/db2025/src/parser/yacc.tab.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (&yylloc, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 641 "/home/nero/diff/db2025/src/parser/yacc.y"
