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
#line 1 "/home/gyl/cpp/db2025/src/parser/yacc.y"

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

#line 103 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"

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
  YYSYMBOL_LOAD = 53,                      /* LOAD  */
  YYSYMBOL_LEQ = 54,                       /* LEQ  */
  YYSYMBOL_NEQ = 55,                       /* NEQ  */
  YYSYMBOL_GEQ = 56,                       /* GEQ  */
  YYSYMBOL_T_EOF = 57,                     /* T_EOF  */
  YYSYMBOL_OUTPUT_FILE = 58,               /* OUTPUT_FILE  */
  YYSYMBOL_OFF = 59,                       /* OFF  */
  YYSYMBOL_IDENTIFIER = 60,                /* IDENTIFIER  */
  YYSYMBOL_VALUE_STRING = 61,              /* VALUE_STRING  */
  YYSYMBOL_VALUE_PATH = 62,                /* VALUE_PATH  */
  YYSYMBOL_VALUE_INT = 63,                 /* VALUE_INT  */
  YYSYMBOL_VALUE_FLOAT = 64,               /* VALUE_FLOAT  */
  YYSYMBOL_VALUE_BOOL = 65,                /* VALUE_BOOL  */
  YYSYMBOL_66_ = 66,                       /* ';'  */
  YYSYMBOL_67_ = 67,                       /* '='  */
  YYSYMBOL_68_ = 68,                       /* '('  */
  YYSYMBOL_69_ = 69,                       /* ')'  */
  YYSYMBOL_70_ = 70,                       /* ','  */
  YYSYMBOL_71_ = 71,                       /* '.'  */
  YYSYMBOL_72_ = 72,                       /* '*'  */
  YYSYMBOL_73_ = 73,                       /* '<'  */
  YYSYMBOL_74_ = 74,                       /* '>'  */
  YYSYMBOL_75_ = 75,                       /* '+'  */
  YYSYMBOL_76_ = 76,                       /* '-'  */
  YYSYMBOL_YYACCEPT = 77,                  /* $accept  */
  YYSYMBOL_start = 78,                     /* start  */
  YYSYMBOL_stmt = 79,                      /* stmt  */
  YYSYMBOL_txnStmt = 80,                   /* txnStmt  */
  YYSYMBOL_dbStmt = 81,                    /* dbStmt  */
  YYSYMBOL_setStmt = 82,                   /* setStmt  */
  YYSYMBOL_io_stmt = 83,                   /* io_stmt  */
  YYSYMBOL_ddl = 84,                       /* ddl  */
  YYSYMBOL_dml = 85,                       /* dml  */
  YYSYMBOL_fieldList = 86,                 /* fieldList  */
  YYSYMBOL_colNameList = 87,               /* colNameList  */
  YYSYMBOL_field = 88,                     /* field  */
  YYSYMBOL_type = 89,                      /* type  */
  YYSYMBOL_valueList = 90,                 /* valueList  */
  YYSYMBOL_value = 91,                     /* value  */
  YYSYMBOL_condition = 92,                 /* condition  */
  YYSYMBOL_optWhereClause = 93,            /* optWhereClause  */
  YYSYMBOL_optJoinClause = 94,             /* optJoinClause  */
  YYSYMBOL_opt_having_clause = 95,         /* opt_having_clause  */
  YYSYMBOL_whereClause = 96,               /* whereClause  */
  YYSYMBOL_col = 97,                       /* col  */
  YYSYMBOL_aggCol = 98,                    /* aggCol  */
  YYSYMBOL_colList = 99,                   /* colList  */
  YYSYMBOL_op = 100,                       /* op  */
  YYSYMBOL_expr = 101,                     /* expr  */
  YYSYMBOL_setClauses = 102,               /* setClauses  */
  YYSYMBOL_setClause = 103,                /* setClause  */
  YYSYMBOL_selector = 104,                 /* selector  */
  YYSYMBOL_tableList = 105,                /* tableList  */
  YYSYMBOL_opt_order_clause = 106,         /* opt_order_clause  */
  YYSYMBOL_opt_limit_clause = 107,         /* opt_limit_clause  */
  YYSYMBOL_opt_groupby_clause = 108,       /* opt_groupby_clause  */
  YYSYMBOL_order_clause = 109,             /* order_clause  */
  YYSYMBOL_order_item = 110,               /* order_item  */
  YYSYMBOL_opt_asc_desc = 111,             /* opt_asc_desc  */
  YYSYMBOL_set_knob_type = 112,            /* set_knob_type  */
  YYSYMBOL_tbName = 113,                   /* tbName  */
  YYSYMBOL_colName = 114,                  /* colName  */
  YYSYMBOL_ALIAS = 115,                    /* ALIAS  */
  YYSYMBOL_fileName = 116                  /* fileName  */
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
#define YYFINAL  59
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   224

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  77
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  40
/* YYNRULES -- Number of rules.  */
#define YYNRULES  115
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  218

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   320


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
      68,    69,    72,    75,    70,    76,    71,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    66,
      73,    67,    74,     2,     2,     2,     2,     2,     2,     2,
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
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    99,    99,   104,   109,   114,   119,   127,   128,   129,
     130,   131,   132,   139,   143,   147,   151,   158,   162,   171,
     177,   181,   187,   193,   198,   203,   209,   215,   220,   227,
     233,   243,   254,   287,   292,   300,   306,   315,   324,   328,
     332,   336,   343,   348,   356,   360,   365,   369,   376,   385,
     386,   393,   394,   401,   402,   409,   415,   425,   431,   436,
     443,   449,   456,   461,   466,   471,   476,   481,   488,   494,
     503,   507,   511,   515,   519,   523,   527,   531,   538,   542,
     549,   555,   564,   569,   575,   581,   587,   593,   602,   606,
     610,   617,   625,   632,   640,   653,   667,   680,   697,   701,
     705,   710,   716,   720,   724,   729,   738,   746,   747,   748,
     752,   753,   756,   757,   758,   759
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
  "SUM", "COUNT", "MAX", "MIN", "AVG", "AS", "LOAD", "LEQ", "NEQ", "GEQ",
  "T_EOF", "OUTPUT_FILE", "OFF", "IDENTIFIER", "VALUE_STRING",
  "VALUE_PATH", "VALUE_INT", "VALUE_FLOAT", "VALUE_BOOL", "';'", "'='",
  "'('", "')'", "','", "'.'", "'*'", "'<'", "'>'", "'+'", "'-'", "$accept",
  "start", "stmt", "txnStmt", "dbStmt", "setStmt", "io_stmt", "ddl", "dml",
  "fieldList", "colNameList", "field", "type", "valueList", "value",
  "condition", "optWhereClause", "optJoinClause", "opt_having_clause",
  "whereClause", "col", "aggCol", "colList", "op", "expr", "setClauses",
  "setClause", "selector", "tableList", "opt_order_clause",
  "opt_limit_clause", "opt_groupby_clause", "order_clause", "order_item",
  "opt_asc_desc", "set_knob_type", "tbName", "colName", "ALIAS",
  "fileName", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-158)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-113)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      94,    11,     4,     7,   -57,    -5,    32,   -57,    -1,   120,
    -158,  -158,    39,  -158,  -158,  -158,  -158,   -13,  -158,    12,
      16,  -158,  -158,  -158,  -158,  -158,  -158,  -158,    48,   -57,
     -57,  -158,   -57,   -57,  -158,  -158,   -57,   -57,    56,  -158,
    -158,   -21,    19,    30,    50,    51,    55,    57,    17,  -158,
    -158,    72,    37,   115,    29,    79,  -158,  -158,   104,  -158,
    -158,   -57,    69,    71,  -158,    76,   129,   125,    86,  -158,
    -158,    83,    62,   134,    62,    62,    62,    90,    62,   -57,
      86,    90,   -57,  -158,    86,    86,    86,    84,    62,  -158,
    -158,   -11,  -158,    91,  -158,   103,   105,   106,   110,   122,
     124,  -158,  -158,  -158,   -14,    90,  -158,  -158,  -158,   -48,
    -158,    46,   -46,  -158,    22,    92,  -158,   166,    87,    86,
    -158,    20,  -158,  -158,  -158,  -158,  -158,  -158,   132,   -57,
     -57,   180,  -158,  -158,    86,  -158,   130,  -158,  -158,  -158,
    -158,    86,  -158,  -158,  -158,  -158,  -158,    35,  -158,    62,
    -158,   168,  -158,  -158,  -158,  -158,  -158,  -158,   139,  -158,
    -158,   101,   -57,   -24,    90,   188,   190,  -158,   146,  -158,
    -158,    92,  -158,  -158,  -158,  -158,  -158,    92,    92,    92,
      92,  -158,   -24,    62,  -158,   178,  -158,    62,    62,   196,
     143,  -158,  -158,  -158,  -158,  -158,  -158,   178,   166,  -158,
      37,   166,   197,   194,  -158,  -158,    62,   152,  -158,    38,
     147,  -158,  -158,  -158,  -158,  -158,    62,  -158
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       4,     3,     0,    13,    14,    15,    16,     0,     5,     0,
       0,    10,     7,    11,     6,     8,     9,    17,     0,     0,
       0,    28,     0,     0,   112,    24,     0,     0,     0,   110,
     111,     0,     0,     0,     0,     0,     0,     0,   113,    88,
      68,    61,    89,     0,     0,    58,    12,   115,     0,     1,
       2,     0,     0,     0,    23,     0,     0,    49,     0,    20,
      21,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    27,     0,     0,     0,     0,     0,    30,
     113,    49,    80,     0,    19,     0,     0,     0,     0,     0,
       0,   114,    60,    69,    49,    90,    57,    59,    18,     0,
      33,     0,     0,    35,     0,     0,    55,    50,     0,     0,
      31,     0,    62,    67,    66,    64,    63,    65,     0,     0,
       0,   103,    91,    22,     0,    38,     0,    40,    41,    37,
      25,     0,    26,    46,    44,    45,    47,     0,    42,     0,
      76,     0,    74,    73,    75,    70,    71,    72,     0,    81,
      82,     0,     0,    51,    92,     0,    53,    34,     0,    36,
      29,     0,    56,    77,    78,    79,    48,     0,     0,     0,
       0,    83,    51,     0,    94,    51,    93,     0,     0,    99,
       0,    43,    87,    86,    84,    85,    96,    51,    52,    95,
     102,    54,     0,   101,    39,    97,     0,     0,    32,   109,
      98,   104,   100,   108,   107,   106,     0,   105
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -158,  -158,  -158,  -158,  -158,  -158,  -158,  -158,   204,  -158,
     133,    88,  -158,  -158,   -84,    74,   -50,  -157,  -158,  -141,
      -9,  -158,    31,  -158,  -158,  -158,   102,  -158,  -158,  -158,
    -158,  -158,  -158,     8,  -158,  -158,    -3,   -66,   -74,  -158
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    19,    20,    21,    22,    23,    24,    25,    26,   109,
     112,   110,   139,   147,   148,   116,    89,   184,   189,   117,
     118,    51,    52,   158,   176,    91,    92,    53,   104,   203,
     208,   166,   210,   211,   215,    42,    54,    55,   102,    58
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      50,    35,    93,    34,    38,    36,    88,   107,   183,    88,
      29,    69,    59,    32,   106,    27,   128,   129,   111,   113,
     113,   133,   134,   140,   141,   196,    62,    63,   199,    64,
      65,   132,    30,    66,    67,    33,   101,   160,    70,    28,
     205,   120,   198,    39,    40,    37,   213,   201,     5,    57,
      31,     6,   214,    93,   131,   161,   130,    41,    83,   119,
       7,    61,     9,    95,    97,    98,    99,   100,   111,   103,
     135,   136,   137,   138,   174,   169,   105,   181,    68,   108,
      90,   143,    60,   144,   145,   146,    71,   191,  -112,   185,
     186,   142,   141,   192,   193,   194,   195,     1,    72,     2,
      80,     3,     4,     5,   170,   171,     6,    78,   197,    43,
      44,    45,    46,    47,    82,     7,     8,     9,    73,    74,
     150,   151,    48,    75,    77,    76,   163,   164,    79,    10,
      11,    81,    12,    13,    14,    15,    16,    84,   177,    85,
      87,   152,   153,   154,    86,    88,    90,    17,    94,   175,
     101,    18,   115,   143,   155,   144,   145,   146,   121,   182,
     156,   157,   143,   162,   144,   145,   146,    43,    44,    45,
      46,    47,   122,   178,   123,   124,   179,   180,    50,   125,
      48,    43,    44,    45,    46,    47,    43,    44,    45,    46,
      47,   126,    49,   127,    48,   149,   165,   209,   168,    48,
     143,   173,   144,   145,   146,   187,    96,   209,   188,   190,
     183,   202,   204,   207,   206,   212,    56,   216,   200,   114,
       0,   159,   167,   172,   217
};

static const yytype_int16 yycheck[] =
{
       9,     4,    68,    60,     7,    10,    20,    81,    32,    20,
       6,    32,     0,     6,    80,     4,    30,    31,    84,    85,
      86,    69,    70,    69,    70,   182,    29,    30,   185,    32,
      33,   105,    28,    36,    37,    28,    60,   121,    59,    28,
     197,    91,   183,    44,    45,    13,     8,   188,     9,    62,
      46,    12,    14,   119,   104,   121,    70,    58,    61,    70,
      21,    13,    23,    72,    73,    74,    75,    76,   134,    78,
      24,    25,    26,    27,   158,   141,    79,   161,    22,    82,
      60,    61,    66,    63,    64,    65,    67,   171,    71,   163,
     164,    69,    70,   177,   178,   179,   180,     3,    68,     5,
      71,     7,     8,     9,    69,    70,    12,    70,   182,    47,
      48,    49,    50,    51,    10,    21,    22,    23,    68,    68,
      33,    34,    60,    68,    52,    68,   129,   130,    13,    35,
      36,    52,    38,    39,    40,    41,    42,    68,    37,    68,
      11,    54,    55,    56,    68,    20,    60,    53,    65,   158,
      60,    57,    68,    61,    67,    63,    64,    65,    67,   162,
      73,    74,    61,    31,    63,    64,    65,    47,    48,    49,
      50,    51,    69,    72,    69,    69,    75,    76,   187,    69,
      60,    47,    48,    49,    50,    51,    47,    48,    49,    50,
      51,    69,    72,    69,    60,    29,    16,   206,    68,    60,
      61,    33,    63,    64,    65,    17,    72,   216,    18,    63,
      32,    15,    69,    19,    17,    63,    12,    70,   187,    86,
      -1,   119,   134,   149,   216
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     5,     7,     8,     9,    12,    21,    22,    23,
      35,    36,    38,    39,    40,    41,    42,    53,    57,    78,
      79,    80,    81,    82,    83,    84,    85,     4,    28,     6,
      28,    46,     6,    28,    60,   113,    10,    13,   113,    44,
      45,    58,   112,    47,    48,    49,    50,    51,    60,    72,
      97,    98,    99,   104,   113,   114,    85,    62,   116,     0,
      66,    13,   113,   113,   113,   113,   113,   113,    22,    32,
      59,    67,    68,    68,    68,    68,    68,    52,    70,    13,
      71,    52,    10,   113,    68,    68,    68,    11,    20,    93,
      60,   102,   103,   114,    65,    97,    72,    97,    97,    97,
      97,    60,   115,    97,   105,   113,   114,   115,   113,    86,
      88,   114,    87,   114,    87,    68,    92,    96,    97,    70,
      93,    67,    69,    69,    69,    69,    69,    69,    30,    31,
      70,    93,   115,    69,    70,    24,    25,    26,    27,    89,
      69,    70,    69,    61,    63,    64,    65,    90,    91,    29,
      33,    34,    54,    55,    56,    67,    73,    74,   100,   103,
      91,   114,    31,   113,   113,    16,   108,    88,    68,   114,
      69,    70,    92,    33,    91,    97,   101,    37,    72,    75,
      76,    91,   113,    32,    94,   115,   115,    17,    18,    95,
      63,    91,    91,    91,    91,    91,    94,   115,    96,    94,
      99,    96,    15,   106,    69,    94,    17,    19,   107,    97,
     109,   110,    63,     8,    14,   111,    70,   110
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    77,    78,    78,    78,    78,    78,    79,    79,    79,
      79,    79,    79,    80,    80,    80,    80,    81,    81,    82,
      83,    83,    84,    84,    84,    84,    84,    84,    84,    85,
      85,    85,    85,    86,    86,    87,    87,    88,    89,    89,
      89,    89,    90,    90,    91,    91,    91,    91,    92,    93,
      93,    94,    94,    95,    95,    96,    96,    97,    97,    97,
      97,    97,    98,    98,    98,    98,    98,    98,    99,    99,
     100,   100,   100,   100,   100,   100,   100,   100,   101,   101,
     102,   102,   103,   103,   103,   103,   103,   103,   104,   104,
     105,   105,   105,   105,   105,   105,   105,   105,   106,   106,
     107,   107,   108,   108,   109,   109,   110,   111,   111,   111,
     112,   112,   113,   114,   115,   116
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     2,     1,     1,     1,     1,     2,     4,     4,
       3,     3,     6,     3,     2,     6,     6,     4,     2,     7,
       4,     5,     9,     1,     3,     1,     3,     2,     1,     4,
       1,     1,     1,     3,     1,     1,     1,     1,     3,     0,
       2,     0,     2,     0,     2,     1,     3,     3,     1,     3,
       3,     1,     4,     4,     4,     4,     4,     4,     1,     3,
       1,     1,     1,     1,     1,     1,     1,     2,     1,     1,
       1,     3,     3,     4,     5,     5,     5,     5,     1,     1,
       1,     2,     3,     4,     4,     5,     5,     6,     3,     0,
       2,     0,     3,     0,     1,     3,     2,     1,     1,     0,
       1,     1,     1,     1,     1,     1
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
  switch (yykind)
    {
    case YYSYMBOL_IDENTIFIER: /* IDENTIFIER  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1461 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_VALUE_STRING: /* VALUE_STRING  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1467 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_VALUE_PATH: /* VALUE_PATH  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1473 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_fieldList: /* fieldList  */
#line 37 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_fields); }
#line 1479 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_colNameList: /* colNameList  */
#line 34 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_strs); }
#line 1485 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_field: /* field  */
#line 36 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_field); }
#line 1491 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_type: /* type  */
#line 35 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_type_len); }
#line 1497 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_valueList: /* valueList  */
#line 40 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_vals); }
#line 1503 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_value: /* value  */
#line 39 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_val); }
#line 1509 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_condition: /* condition  */
#line 45 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_cond); }
#line 1515 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_optWhereClause: /* optWhereClause  */
#line 46 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_conds); }
#line 1521 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_optJoinClause: /* optJoinClause  */
#line 46 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_conds); }
#line 1527 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_opt_having_clause: /* opt_having_clause  */
#line 46 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_conds); }
#line 1533 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_whereClause: /* whereClause  */
#line 46 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_conds); }
#line 1539 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_col: /* col  */
#line 41 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_col); }
#line 1545 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_aggCol: /* aggCol  */
#line 41 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_col); }
#line 1551 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_colList: /* colList  */
#line 42 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_cols); }
#line 1557 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_expr: /* expr  */
#line 38 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_expr); }
#line 1563 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_setClauses: /* setClauses  */
#line 44 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_set_clauses); }
#line 1569 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_setClause: /* setClause  */
#line 43 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_set_clause); }
#line 1575 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_selector: /* selector  */
#line 42 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_cols); }
#line 1581 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_tableList: /* tableList  */
#line 49 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_table_list); }
#line 1587 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_opt_order_clause: /* opt_order_clause  */
#line 47 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_orderby); }
#line 1593 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_opt_groupby_clause: /* opt_groupby_clause  */
#line 42 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_cols); }
#line 1599 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_order_clause: /* order_clause  */
#line 47 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_orderby); }
#line 1605 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_order_item: /* order_item  */
#line 48 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_order_item); }
#line 1611 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_tbName: /* tbName  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1617 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_colName: /* colName  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1623 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_ALIAS: /* ALIAS  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1629 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

    case YYSYMBOL_fileName: /* fileName  */
#line 33 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { delete ((*yyvaluep).sv_str); }
#line 1635 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
        break;

      default:
        break;
    }
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
#line 100 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        parse_tree = std::shared_ptr<ast::TreeNode>((yyvsp[-1].sv_node));
        YYACCEPT;
    }
#line 1944 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 3: /* start: HELP  */
#line 105 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
#line 1953 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 4: /* start: EXIT  */
#line 110 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1962 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 5: /* start: T_EOF  */
#line 115 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1971 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 6: /* start: io_stmt  */
#line 120 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        parse_tree = std::shared_ptr<ast::TreeNode>((yyvsp[0].sv_node));
        YYACCEPT;
    }
#line 1980 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 12: /* stmt: EXPLAIN dml  */
#line 133 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new ExplainStmt((yyvsp[0].sv_node));
    }
#line 1988 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 13: /* txnStmt: TXN_BEGIN  */
#line 140 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new TxnBegin;
    }
#line 1996 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 14: /* txnStmt: TXN_COMMIT  */
#line 144 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new TxnCommit;
    }
#line 2004 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 15: /* txnStmt: TXN_ABORT  */
#line 148 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new TxnAbort;
    }
#line 2012 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 16: /* txnStmt: TXN_ROLLBACK  */
#line 152 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new TxnRollback;
    }
#line 2020 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 17: /* dbStmt: SHOW TABLES  */
#line 159 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new ShowTables;
    }
#line 2028 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 18: /* dbStmt: LOAD fileName INTO tbName  */
#line 163 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new LoadStmt(*(yyvsp[-2].sv_str), *(yyvsp[0].sv_str));
        delete (yyvsp[-2].sv_str);
        delete (yyvsp[0].sv_str);
    }
#line 2038 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 19: /* setStmt: SET set_knob_type '=' VALUE_BOOL  */
#line 172 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new SetStmt((yyvsp[-2].sv_setKnobType), (yyvsp[0].sv_bool));
    }
#line 2046 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 20: /* io_stmt: SET OUTPUT_FILE ON  */
#line 178 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new IoEnable(true);
    }
#line 2054 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 21: /* io_stmt: SET OUTPUT_FILE OFF  */
#line 182 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new IoEnable(false);
    }
#line 2062 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 22: /* ddl: CREATE TABLE tbName '(' fieldList ')'  */
#line 188 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new CreateTable(*(yyvsp[-3].sv_str), *(yyvsp[-1].sv_fields));
        delete (yyvsp[-3].sv_str);
        delete (yyvsp[-1].sv_fields);
    }
#line 2072 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 23: /* ddl: DROP TABLE tbName  */
#line 194 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new DropTable(*(yyvsp[0].sv_str));
        delete (yyvsp[0].sv_str);
    }
#line 2081 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 24: /* ddl: DESC tbName  */
#line 199 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new DescTable(*(yyvsp[0].sv_str));
        delete (yyvsp[0].sv_str);
    }
#line 2090 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 25: /* ddl: CREATE INDEX tbName '(' colNameList ')'  */
#line 204 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new CreateIndex(*(yyvsp[-3].sv_str), *(yyvsp[-1].sv_strs));
        delete (yyvsp[-3].sv_str);
        delete (yyvsp[-1].sv_strs);
    }
#line 2100 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 26: /* ddl: DROP INDEX tbName '(' colNameList ')'  */
#line 210 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new DropIndex(*(yyvsp[-3].sv_str), *(yyvsp[-1].sv_strs));
        delete (yyvsp[-3].sv_str);
        delete (yyvsp[-1].sv_strs);
    }
#line 2110 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 27: /* ddl: SHOW INDEX FROM tbName  */
#line 216 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new ShowIndex(*(yyvsp[0].sv_str));
        delete (yyvsp[0].sv_str);
    }
#line 2119 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 28: /* ddl: CREATE STATIC_CHECKPOINT  */
#line 221 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new CreateStaticCheckpoint;
    }
#line 2127 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 29: /* dml: INSERT INTO tbName VALUES '(' valueList ')'  */
#line 228 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_node) = new InsertStmt(*(yyvsp[-4].sv_str), *(yyvsp[-1].sv_vals));
        delete (yyvsp[-4].sv_str);
        delete (yyvsp[-1].sv_vals);
    }
#line 2137 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 30: /* dml: DELETE FROM tbName optWhereClause  */
#line 234 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        DeleteStmt* delete_stmt = new DeleteStmt(*(yyvsp[-1].sv_str));
        if((yyvsp[0].sv_conds) != nullptr) {
            delete_stmt->conds = std::move(*(yyvsp[0].sv_conds));
            delete (yyvsp[0].sv_conds);
        }
        delete (yyvsp[-1].sv_str);
        (yyval.sv_node) = delete_stmt;
    }
#line 2151 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 31: /* dml: UPDATE tbName SET setClauses optWhereClause  */
#line 244 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        UpdateStmt* update_stmt = new UpdateStmt(*(yyvsp[-3].sv_str), *(yyvsp[-1].sv_set_clauses));
        if((yyvsp[0].sv_conds) != nullptr) {
            update_stmt->conds = std::move(*(yyvsp[0].sv_conds));
            delete (yyvsp[0].sv_conds);
        }
        delete (yyvsp[-1].sv_set_clauses);
        delete (yyvsp[-3].sv_str);
        (yyval.sv_node) = update_stmt;
    }
#line 2166 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 32: /* dml: SELECT selector FROM tableList optWhereClause opt_groupby_clause opt_having_clause opt_order_clause opt_limit_clause  */
#line 255 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        // 例如在 SelectStmt 创建时
        SelectStmt* select_stmt = new SelectStmt(
            *(yyvsp[-7].sv_cols),             // selector
            (yyvsp[-5].sv_table_list)->tables,      // 表列表
            (yyvsp[-5].sv_table_list)->jointree,    // 连接树
            (yyvsp[0].sv_int)               // limit (int，不需要move)
        );
        select_stmt->tab_aliases = std::move((yyvsp[-5].sv_table_list)->aliases);      // 表别名
        if((yyvsp[-4].sv_conds) != nullptr) {
            select_stmt->conds = std::move(*(yyvsp[-4].sv_conds));
            delete (yyvsp[-4].sv_conds);
        }
        if((yyvsp[-3].sv_cols) != nullptr) {
            select_stmt->groupby = std::move(*(yyvsp[-3].sv_cols));
            delete (yyvsp[-3].sv_cols);
        }
        if((yyvsp[-2].sv_conds) != nullptr) {
            select_stmt->having_conds = std::move(*(yyvsp[-2].sv_conds));
            delete (yyvsp[-2].sv_conds);
        }
        if((yyvsp[-1].sv_orderby) != nullptr) {
            select_stmt->order = std::move(*(yyvsp[-1].sv_orderby));
            delete (yyvsp[-1].sv_orderby);
        }
        delete (yyvsp[-7].sv_cols);
        delete (yyvsp[-5].sv_table_list);
        (yyval.sv_node) = select_stmt;
    }
#line 2200 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 33: /* fieldList: field  */
#line 288 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_fields) = new std::vector<std::unique_ptr<Field>>();
        (yyval.sv_fields)->emplace_back((yyvsp[0].sv_field));
    }
#line 2209 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 34: /* fieldList: fieldList ',' field  */
#line 293 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_fields) = (yyvsp[-2].sv_fields);
        (yyval.sv_fields)->emplace_back((yyvsp[0].sv_field));
    }
#line 2218 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 35: /* colNameList: colName  */
#line 301 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_strs) = new std::vector<std::string>(); // 使用 move
        (yyval.sv_strs)->emplace_back(std::move(*(yyvsp[0].sv_str)));
        delete (yyvsp[0].sv_str);
    }
#line 2228 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 36: /* colNameList: colNameList ',' colName  */
#line 307 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_strs) = (yyvsp[-2].sv_strs);
        (yyval.sv_strs)->emplace_back(std::move(*(yyvsp[0].sv_str))); // 使用 move
        delete (yyvsp[0].sv_str);
    }
#line 2238 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 37: /* field: colName type  */
#line 316 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_field) = new ColDef(*(yyvsp[-1].sv_str), *(yyvsp[0].sv_type_len));
        delete (yyvsp[-1].sv_str);
        delete (yyvsp[0].sv_type_len);
    }
#line 2248 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 38: /* type: INT  */
#line 325 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = new TypeLen(SV_TYPE_INT, sizeof(int));
    }
#line 2256 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 39: /* type: CHAR '(' VALUE_INT ')'  */
#line 329 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = new TypeLen(SV_TYPE_STRING, (yyvsp[-1].sv_int));
    }
#line 2264 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 40: /* type: FLOAT  */
#line 333 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = new TypeLen(SV_TYPE_FLOAT, sizeof(float));
    }
#line 2272 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 41: /* type: DATETIME  */
#line 337 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = new TypeLen(SV_TYPE_DATETIME, 19);
    }
#line 2280 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 42: /* valueList: value  */
#line 344 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_vals) = new std::vector<std::unique_ptr<Value>>();
        (yyval.sv_vals)->emplace_back((yyvsp[0].sv_val));
    }
#line 2289 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 43: /* valueList: valueList ',' value  */
#line 349 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_vals) = (yyvsp[-2].sv_vals);
        (yyval.sv_vals)->emplace_back((yyvsp[0].sv_val)); // 使用 move
    }
#line 2298 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 44: /* value: VALUE_INT  */
#line 357 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = new IntLit((yyvsp[0].sv_int));
    }
#line 2306 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 45: /* value: VALUE_FLOAT  */
#line 361 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        // 浮点数在词法分析阶段已经进行了精度处理
        (yyval.sv_val) = new FloatLit((yyvsp[0].sv_float));
    }
#line 2315 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 46: /* value: VALUE_STRING  */
#line 366 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = new StringLit(*(yyvsp[0].sv_str));
    }
#line 2323 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 47: /* value: VALUE_BOOL  */
#line 370 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_val) = new BoolLit((yyvsp[0].sv_bool));
    }
#line 2331 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 48: /* condition: col op expr  */
#line 377 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cond) = new BinaryExpr(*(yyvsp[-2].sv_col), (yyvsp[-1].sv_comp_op), (yyvsp[0].sv_expr));
        delete (yyvsp[-2].sv_col);
    }
#line 2340 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 49: /* optWhereClause: %empty  */
#line 385 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                      { (yyval.sv_conds)=nullptr; }
#line 2346 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 50: /* optWhereClause: WHERE whereClause  */
#line 387 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2354 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 51: /* optJoinClause: %empty  */
#line 393 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                      { (yyval.sv_conds)=nullptr; }
#line 2360 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 52: /* optJoinClause: ON whereClause  */
#line 395 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2368 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 53: /* opt_having_clause: %empty  */
#line 401 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                  { (yyval.sv_conds)=nullptr; }
#line 2374 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 54: /* opt_having_clause: HAVING whereClause  */
#line 403 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2382 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 55: /* whereClause: condition  */
#line 410 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = new std::vector<BinaryExpr>(); // 使用 move
        (yyval.sv_conds)->emplace_back(std::move(*(yyvsp[0].sv_cond)));
        delete (yyvsp[0].sv_cond);
    }
#line 2392 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 56: /* whereClause: whereClause AND condition  */
#line 416 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[-2].sv_conds);
        (yyval.sv_conds)->emplace_back(std::move(*(yyvsp[0].sv_cond))); // 使用 move
        delete (yyvsp[0].sv_cond);
    }
#line 2402 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 57: /* col: tbName '.' colName  */
#line 426 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = new Col(*(yyvsp[-2].sv_str), *(yyvsp[0].sv_str));
        delete (yyvsp[-2].sv_str);
        delete (yyvsp[0].sv_str);
    }
#line 2412 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 58: /* col: colName  */
#line 432 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = new Col(*(yyvsp[0].sv_str));
        delete (yyvsp[0].sv_str);
    }
#line 2421 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 59: /* col: colName AS ALIAS  */
#line 437 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = new Col(*(yyvsp[-2].sv_str));
        (yyval.sv_col)->alias = std::move(*(yyvsp[0].sv_str));
        delete (yyvsp[-2].sv_str);
        delete (yyvsp[0].sv_str);
    }
#line 2432 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 60: /* col: aggCol AS ALIAS  */
#line 444 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-2].sv_col);
        (yyval.sv_col)->alias = std::move(*(yyvsp[0].sv_str));
        delete (yyvsp[0].sv_str);
    }
#line 2442 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 61: /* col: aggCol  */
#line 450 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[0].sv_col);
    }
#line 2450 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 62: /* aggCol: SUM '(' col ')'  */
#line 457 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-1].sv_col);
        (yyval.sv_col)->agg_type = AggFuncType::SUM;
    }
#line 2459 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 63: /* aggCol: MIN '(' col ')'  */
#line 462 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-1].sv_col);
        (yyval.sv_col)->agg_type = AggFuncType::MIN;
    }
#line 2468 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 64: /* aggCol: MAX '(' col ')'  */
#line 467 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-1].sv_col);
        (yyval.sv_col)->agg_type = AggFuncType::MAX;
    }
#line 2477 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 65: /* aggCol: AVG '(' col ')'  */
#line 472 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-1].sv_col);
        (yyval.sv_col)->agg_type = AggFuncType::AVG;
    }
#line 2486 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 66: /* aggCol: COUNT '(' col ')'  */
#line 477 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = (yyvsp[-1].sv_col);
        (yyval.sv_col)->agg_type = AggFuncType::COUNT;
    }
#line 2495 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 67: /* aggCol: COUNT '(' '*' ')'  */
#line 482 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_col) = new Col("", "*", AggFuncType::COUNT);
    }
#line 2503 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 68: /* colList: col  */
#line 489 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = new std::vector<Col>(); // 使用 move
        (yyval.sv_cols)->emplace_back(std::move(*(yyvsp[0].sv_col)));
        delete (yyvsp[0].sv_col);
    }
#line 2513 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 69: /* colList: colList ',' col  */
#line 495 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = (yyvsp[-2].sv_cols);
        (yyval.sv_cols)->emplace_back(std::move(*(yyvsp[0].sv_col))); // 使用 move
        delete (yyvsp[0].sv_col);
    }
#line 2523 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 70: /* op: '='  */
#line 504 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_EQ;
    }
#line 2531 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 71: /* op: '<'  */
#line 508 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LT;
    }
#line 2539 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 72: /* op: '>'  */
#line 512 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GT;
    }
#line 2547 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 73: /* op: NEQ  */
#line 516 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_NE;
    }
#line 2555 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 74: /* op: LEQ  */
#line 520 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LE;
    }
#line 2563 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 75: /* op: GEQ  */
#line 524 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GE;
    }
#line 2571 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 76: /* op: IN  */
#line 528 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
	    (yyval.sv_comp_op) = SV_OP_IN;
    }
#line 2579 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 77: /* op: NOT IN  */
#line 532 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
    	(yyval.sv_comp_op) = SV_OP_NOT_IN;
    }
#line 2587 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 78: /* expr: value  */
#line 539 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_expr) = (yyvsp[0].sv_val);
    }
#line 2595 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 79: /* expr: col  */
#line 543 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_expr) = (yyvsp[0].sv_col);
    }
#line 2603 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 80: /* setClauses: setClause  */
#line 550 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses) = new std::vector<SetClause>(); // 使用 move
        (yyval.sv_set_clauses)->emplace_back(std::move(*(yyvsp[0].sv_set_clause)));
        delete (yyvsp[0].sv_set_clause);
    }
#line 2613 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 81: /* setClauses: setClauses ',' setClause  */
#line 556 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses) = (yyvsp[-2].sv_set_clauses);
        (yyval.sv_set_clauses)->emplace_back(std::move(*(yyvsp[0].sv_set_clause))); // 使用 move
        delete (yyvsp[0].sv_set_clause);
    }
#line 2623 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 82: /* setClause: colName '=' value  */
#line 565 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = new SetClause(*(yyvsp[-2].sv_str), (yyvsp[0].sv_val), UpdateOp::ASSINGMENT);
        delete (yyvsp[-2].sv_str);
    }
#line 2632 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 83: /* setClause: colName '=' colName value  */
#line 570 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = new SetClause(*(yyvsp[-3].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_ADD);
        delete (yyvsp[-3].sv_str);
        delete (yyvsp[-1].sv_str);
    }
#line 2642 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 84: /* setClause: colName '=' colName '+' value  */
#line 576 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = new SetClause(*(yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_ADD);
        delete (yyvsp[-4].sv_str);
        delete (yyvsp[-2].sv_str);
    }
#line 2652 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 85: /* setClause: colName '=' colName '-' value  */
#line 582 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = new SetClause(*(yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_SUB);
        delete (yyvsp[-4].sv_str);
        delete (yyvsp[-2].sv_str);
    }
#line 2662 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 86: /* setClause: colName '=' colName '*' value  */
#line 588 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = new SetClause(*(yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_MUT);
        delete (yyvsp[-4].sv_str);
        delete (yyvsp[-2].sv_str);
    }
#line 2672 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 87: /* setClause: colName '=' colName DIV value  */
#line 594 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = new SetClause(*(yyvsp[-4].sv_str), (yyvsp[0].sv_val), UpdateOp::SELF_DIV);
        delete (yyvsp[-4].sv_str);
        delete (yyvsp[-2].sv_str);
    }
#line 2682 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 88: /* selector: '*'  */
#line 603 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = new std::vector<Col>();
    }
#line 2690 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 90: /* tableList: tbName  */
#line 611 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = new TableList;
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[0].sv_str))); // 使用 move
        (yyval.sv_table_list)->aliases.emplace_back("");
        delete (yyvsp[0].sv_str);
    }
#line 2701 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 91: /* tableList: tbName ALIAS  */
#line 618 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = new TableList;
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[-1].sv_str))); // 使用 move
        (yyval.sv_table_list)->aliases.emplace_back(std::move(*(yyvsp[0].sv_str))); // 使用 move
        delete (yyvsp[-1].sv_str);
        delete (yyvsp[0].sv_str);
    }
#line 2713 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 92: /* tableList: tableList ',' tbName  */
#line 626 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = (yyvsp[-2].sv_table_list);
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[0].sv_str))); // 使用 move
        (yyval.sv_table_list)->aliases.emplace_back("");
        delete (yyvsp[0].sv_str);
    }
#line 2724 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 93: /* tableList: tableList ',' tbName ALIAS  */
#line 633 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = (yyvsp[-3].sv_table_list);
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[-1].sv_str))); // 使用 move
        (yyval.sv_table_list)->aliases.emplace_back(std::move(*(yyvsp[0].sv_str))); // 使用 move
        delete (yyvsp[-1].sv_str);
        delete (yyvsp[0].sv_str);
    }
#line 2736 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 94: /* tableList: tableList JOIN tbName optJoinClause  */
#line 641 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = (yyvsp[-3].sv_table_list);

        (yyval.sv_table_list)->jointree.emplace_back((yyvsp[-3].sv_table_list)->tables.back(), *(yyvsp[-1].sv_str), INNER_JOIN);
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[-1].sv_str)));
        (yyval.sv_table_list)->aliases.emplace_back("");
        delete (yyvsp[-1].sv_str);
        if((yyvsp[0].sv_conds) != nullptr) {
            (yyval.sv_table_list)->jointree.back().conds = std::move(*(yyvsp[0].sv_conds));
            delete (yyvsp[0].sv_conds);
        }
    }
#line 2753 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 95: /* tableList: tableList JOIN tbName ALIAS optJoinClause  */
#line 654 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = (yyvsp[-4].sv_table_list);

        (yyval.sv_table_list)->jointree.emplace_back((yyvsp[-4].sv_table_list)->tables.back(), *(yyvsp[-2].sv_str), INNER_JOIN);
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[-2].sv_str)));
        (yyval.sv_table_list)->aliases.emplace_back(std::move(*(yyvsp[-1].sv_str)));
        delete (yyvsp[-2].sv_str);
        delete (yyvsp[-1].sv_str);
        if((yyvsp[0].sv_conds) != nullptr) {
            (yyval.sv_table_list)->jointree.back().conds = std::move(*(yyvsp[0].sv_conds));
            delete (yyvsp[0].sv_conds);
        }
    }
#line 2771 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 96: /* tableList: tableList SEMI JOIN tbName optJoinClause  */
#line 668 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = (yyvsp[-4].sv_table_list);

        (yyval.sv_table_list)->jointree.emplace_back((yyvsp[-4].sv_table_list)->tables.back(), *(yyvsp[-1].sv_str), SEMI_JOIN);
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[-1].sv_str)));
        (yyval.sv_table_list)->aliases.emplace_back("");
        delete (yyvsp[-1].sv_str);
        if((yyvsp[0].sv_conds) != nullptr) {
            (yyval.sv_table_list)->jointree.back().conds = std::move(*(yyvsp[0].sv_conds));
            delete (yyvsp[0].sv_conds);
        }
    }
#line 2788 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 97: /* tableList: tableList SEMI JOIN tbName ALIAS optJoinClause  */
#line 681 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_table_list) = (yyvsp[-5].sv_table_list);

        (yyval.sv_table_list)->jointree.emplace_back((yyvsp[-5].sv_table_list)->tables.back(), *(yyvsp[-2].sv_str), SEMI_JOIN);
        (yyval.sv_table_list)->tables.emplace_back(std::move(*(yyvsp[-2].sv_str)));
        (yyval.sv_table_list)->aliases.emplace_back(std::move(*(yyvsp[-1].sv_str)));
        delete (yyvsp[-2].sv_str);
        delete (yyvsp[-1].sv_str);
        if((yyvsp[0].sv_conds) != nullptr) {
            (yyval.sv_table_list)->jointree.back().conds = std::move(*(yyvsp[0].sv_conds));
            delete (yyvsp[0].sv_conds);
        }
    }
#line 2806 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 98: /* opt_order_clause: ORDER BY order_clause  */
#line 698 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_orderby) = (yyvsp[0].sv_orderby);
    }
#line 2814 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 99: /* opt_order_clause: %empty  */
#line 701 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                      { (yyval.sv_orderby)=nullptr; }
#line 2820 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 100: /* opt_limit_clause: LIMIT VALUE_INT  */
#line 706 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_int) = (yyvsp[0].sv_int);
    }
#line 2828 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 101: /* opt_limit_clause: %empty  */
#line 710 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_int) = -1;
    }
#line 2836 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 102: /* opt_groupby_clause: GROUP BY colList  */
#line 717 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_cols) = (yyvsp[0].sv_cols);
    }
#line 2844 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 103: /* opt_groupby_clause: %empty  */
#line 720 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                      { (yyval.sv_cols)=nullptr; }
#line 2850 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 104: /* order_clause: order_item  */
#line 725 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_orderby) = new OrderBy((yyvsp[0].sv_order_item)->first, (yyvsp[0].sv_order_item)->second);
        delete (yyvsp[0].sv_order_item);
    }
#line 2859 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 105: /* order_clause: order_clause ',' order_item  */
#line 730 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyvsp[-2].sv_orderby)->addItem((yyvsp[0].sv_order_item)->first, (yyvsp[0].sv_order_item)->second);
        (yyval.sv_orderby) = (yyvsp[-2].sv_orderby);  // 使用 move
        delete (yyvsp[0].sv_order_item);
    }
#line 2869 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 106: /* order_item: col opt_asc_desc  */
#line 739 "/home/gyl/cpp/db2025/src/parser/yacc.y"
    {
        (yyval.sv_order_item) = new std::pair<Col, OrderByDir>(std::move(*(yyvsp[-1].sv_col)), (yyvsp[0].sv_orderby_dir));
        delete (yyvsp[-1].sv_col);
    }
#line 2878 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 107: /* opt_asc_desc: ASC  */
#line 746 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                 { (yyval.sv_orderby_dir) = OrderBy_ASC;     }
#line 2884 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 108: /* opt_asc_desc: DESC  */
#line 747 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                 { (yyval.sv_orderby_dir) = OrderBy_DESC;    }
#line 2890 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 109: /* opt_asc_desc: %empty  */
#line 748 "/home/gyl/cpp/db2025/src/parser/yacc.y"
            { (yyval.sv_orderby_dir) = OrderBy_DEFAULT; }
#line 2896 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 110: /* set_knob_type: ENABLE_NESTLOOP  */
#line 752 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                    { (yyval.sv_setKnobType) = ast::SetKnobType::EnableNestLoop; }
#line 2902 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 111: /* set_knob_type: ENABLE_SORTMERGE  */
#line 753 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                         { (yyval.sv_setKnobType) = ast::SetKnobType::EnableSortMerge; }
#line 2908 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 112: /* tbName: IDENTIFIER  */
#line 756 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                   { (yyval.sv_str) = (yyvsp[0].sv_str); }
#line 2914 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 113: /* colName: IDENTIFIER  */
#line 757 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                    { (yyval.sv_str) = (yyvsp[0].sv_str); }
#line 2920 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 114: /* ALIAS: IDENTIFIER  */
#line 758 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                  { (yyval.sv_str) = (yyvsp[0].sv_str); }
#line 2926 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;

  case 115: /* fileName: VALUE_PATH  */
#line 759 "/home/gyl/cpp/db2025/src/parser/yacc.y"
                     { (yyval.sv_str) = (yyvsp[0].sv_str); }
#line 2932 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"
    break;


#line 2936 "/home/gyl/cpp/db2025/src/parser/yacc.tab.cpp"

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

#line 761 "/home/gyl/cpp/db2025/src/parser/yacc.y"
