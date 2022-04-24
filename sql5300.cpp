#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "heap_storage.h"

// CREATE A DIRECTORY IN YOUR HOME DIR ~/cpsc5300/data before running this
const char *HOME = "cpsc5300/data";
const char *EXAMPLE = "example.db";
const char *FILENAME = "example_data";
const unsigned int BLOCK_SZ = 4096;
DbEnv *_DB_ENV;

using namespace std;
using namespace hsql;

string execute(const SQLStatement *stmt);
string executeSelect(const SelectStatement *stmt);
string executeCreate(const CreateStatement*stmt);
string columnDefinitionToString(const ColumnDefinition *col);
string getExpression(const Expr* expr);
string tableInfoDefinitionToString(const TableRef* table);
string OperatorExpressionToString(const Expr* expr);
string getJoinType(JoinType type);

int main(int argc, char** argv) {
    if (argc != 2){
        return 1;
    }
    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + argv[1];
    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
    _DB_ENV = &env;

    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ);                                               // Set record length to 4K
    db.open(NULL, EXAMPLE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

    // SQL entry
    while (true)
    {
        cout << "SQL> ";
        string sql;
        getline(cin, sql);

        if (sql == "quit"){
            break;
        }

        if (sql.length() == 0){
            continue;
        }

        if (sql == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            cout << "test_heap_file: " << (test_heap_file(FILENAME) ? "ok" : "failed") << endl;
            continue;
        }

        // Use SQLParser
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(sql);

        if (!result->isValid())
        {
            cout << "inValid SQL:" << sql << endl;
            cout << result->errorMsg() << endl;
            delete result;
            continue;
        }
        else
        {
            for (uint i = 0; i < result->size(); i++)
            {
                cout << execute(result->getStatement(i)) << endl;
            }
        }
        delete result;
    }
    return EXIT_SUCCESS;
}

// Sqlhelper function to handle different SQL
string execute(const SQLStatement *stmt)
{
    switch (stmt->type())
    {
        case kStmtSelect:
            return executeSelect((const SelectStatement *)stmt);
            break;
        case kStmtCreate:
            return executeCreate((const CreateStatement*)stmt);
            break;
        default:
            return "No implemented";
            break;
    }
}

/**
 * Convert the select statement into equivalent SQL
 * @param stmt  select statement to unparse
 * @return     equivalent SQL select
 */
string executeSelect(const SelectStatement *stmt)
{
    string selectQuery;
    selectQuery.append("SELECT ");

    for (uint i = 0; i < stmt->selectList->size(); ++i) {
        if (i != 0) {
            selectQuery.append(", ");
        }
        selectQuery.append(getExpression(stmt->selectList->at(i)));
    }

    selectQuery.append(" FROM ");
    selectQuery.append(tableInfoDefinitionToString(stmt->fromTable));

    if (stmt->whereClause != NULL) {
        selectQuery.append(" WHERE ");
        selectQuery.append(getExpression(stmt->whereClause));
    }

    return selectQuery;
}

/**
 * Convert the create statement into equivalent SQL
 * @param stmt  create statement to unparse
 * @return     SQL create
 */
string executeCreate(const CreateStatement *stmt)
{
    string createQuery;
    createQuery.append("CREATE TABLE ");
    createQuery.append(string(stmt->tableName));
    createQuery.append(" (");

    for(uint i = 0; i < stmt->columns->size(); ++i)
    {
        if (i != 0){
            createQuery.append(", ");
        }
        createQuery.append(columnDefinitionToString(stmt->columns->at(i)));
    }

    createQuery.append(")");
    return createQuery;
}


/**
 * get join type
 * @param type  join type to unparse
 * @return     join type
 */
string getJoinType(JoinType type) {
    string joinType;

    switch (type)
    {
    case kJoinInner:
        joinType.append(" JOIN ");
        break;
    case kJoinOuter:
        joinType.append(" OUTER JOIN ");
        break;
    case kJoinLeft:
        joinType.append(" LEFT JOIN ");
        break;
    case kJoinRight:
        joinType.append(" RIGHT JOIN ");
        break;
    case kJoinLeftOuter:
        joinType.append(" LEFT OUTER JOIN ");
        break;
    case kJoinRightOuter:
        joinType.append(" RIGHT OUTER JOIN ");
        break;
    case kJoinCross:
        joinType.append(" CROSS JOIN ");
        break;
    default:
        cout << "Join type "<< type << " not found" << endl;
        break;
    }

    return joinType;
}

string tableInfoDefinitionToString(const TableRef* table) {
    string tableRefInfo;

    switch (table->type) {
    case kTableName:
        tableRefInfo.append(table->name);
        break;
    case kTableSelect:
        tableRefInfo.append(executeSelect(table->select));
        break;
    case kTableJoin:
        tableRefInfo.append(tableInfoDefinitionToString(table->join->left));
        tableRefInfo.append(getJoinType(table->join->type));     
        tableRefInfo.append(tableInfoDefinitionToString(table->join->right));

        if (table->join->condition != NULL)
        {
            tableRefInfo.append(" ON ");
            tableRefInfo.append(getExpression(table->join->condition));
        }
        break;
    case kTableCrossProduct:
        for (uint i = 0; i < table->list->size(); ++i){
            if (i!= 0){
                tableRefInfo.append(", ");
            }
            
            tableRefInfo.append(tableInfoDefinitionToString(table->list->at(i)));
        }
        break;
    }

    if (table->alias != NULL){
        tableRefInfo.append(" AS ");
        tableRefInfo.append(table->alias);
    }

    return tableRefInfo;
}

string getExpression(const Expr* expr) {
    string expression;

    switch (expr->type) {
    case kExprStar:
        expression.append("*");
        break;
    case kExprColumnRef:
        if (expr->table != NULL) {
            expression.append(string(expr->table));
            expression.append(".");
        }           
        expression.append(expr->name);
        break;
        case kExprLiteralFloat:
        expression.append(std::to_string(expr->fval));
        break;
    case kExprLiteralInt:
        expression.append(std::to_string(expr->ival));
        break;
    case kExprLiteralString:
        expression.append(string(expr->name));
        break;
    case kExprFunctionRef:
        expression.append(string(expr->name));
        expression.append(string(expr->expr->name));
        break;
    case kExprOperator:
        expression.append(OperatorExpressionToString(expr));
        break;
    default:
        expression.append("Unrecognized expression type");
        return expression;
    }

    if (expr->alias != NULL) {
       expression.append("AS");
        expression.append(string(expr->alias));
    }
    return expression;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch(col->type) {
        case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

string OperatorExpressionToString(const Expr* expr)
{
    string op;
    if (expr == NULL)
        return "null";
    if (expr->expr != NULL)
        op += getExpression(expr->expr) + " ";

    switch (expr->opType)
    {
    case Expr::SIMPLE_OP:
        op += expr->opChar;
        break;
    case Expr::AND:
        op += "AND";
        break;
    case Expr::OR:
        op += "OR";
        break;
    case Expr::NOT:
        op += "NOT";
        break;
    default:
        op += expr->opType;
        break;
    }

    if (expr->expr2 != NULL)
        op += " " + getExpression(expr->expr2);

    return op;
}
