/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres)
{
    if (qres.column_names != nullptr)
    {
        for (auto const &column_name : *qres.column_names)
            out << column_name << " ";
        out << endl
            << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row : *qres.rows)
        {
            for (auto const &column_name : *qres.column_names)
            {
                Value value = row->at(column_name);
                switch (value.data_type)
                {
                case ColumnAttribute::INT:
                    out << value.n;
                    break;
                case ColumnAttribute::TEXT:
                    out << "\"" << value.s << "\"";
                    break;
                default:
                    out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult()
{
    delete column_names;
    delete column_attributes;
    delete rows;
}

QueryResult *SQLExec::execute(const SQLStatement *statement)
{
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
    {
        SQLExec::tables = new Tables();
    }

    try
    {
        switch (statement->type())
        {
        case kStmtCreate:
            return create((const CreateStatement *)statement);
        case kStmtDrop:
            return drop((const DropStatement *)statement);
        case kStmtShow:
            return show((const ShowStatement *)statement);
        default:
            return new QueryResult("not implemented");
        }
    }
    catch (DbRelationError &e)
    {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute)
{
    column_name = col->name;

    if (col->type == hsql::ColumnDefinition::DataType::TEXT)
    {
        column_attribute = ColumnAttribute::DataType::TEXT;
    }
    else if (col->type == hsql::ColumnDefinition::DataType::INT)
    {
        column_attribute = ColumnAttribute::DataType::INT;
    }
    else
    {
        throw new SQLExecError("Column type not supported " + col->type);
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement)
{
    // get table and columns from the sql statement
    Identifier table_name = statement->tableName;
    // add table name to Tables
    ValueDict row;
    row["table_name"] = Value(table_name);
    Handle tableHandle = SQLExec::tables->insert(&row);
    // add columns to Column
    Handles columnHandles;
    try
    {
        DbRelation &columns_table = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            Identifier column_name;
            ColumnAttribute column_attribute;
            // add everything to Column
            // get all columns names and datatypes
            for (ColumnDefinition *column : *statement->columns)
            {
                column_definition(column, column_name, column_attribute);
                string value = column_attribute.get_data_type() == ColumnAttribute::TEXT ? "TEXT" : "INT";
                row["data_type"] = Value(value);
                row["column_name"] = Value(column_name);
                columnHandles.push_back(columns_table.insert(&row)); 
            }

            // now that columns were successfully added, get table and create it
            DbRelation &table = SQLExec::tables->get_table(table_name);
            table.create();
        }
        catch (DbRelationError &e) {
            try {
                // undo insertions into _columns
                for(auto const columnHandle : columnHandles) {
                    columns_table.del(columnHandle);
                }
            }
            catch (DbRelationError &e) {}
            // throw the exception for the next one to catch
            throw;
        }
    }
    catch (DbRelationError &e)
    {
        try
        {
            // undo table insertion
            SQLExec::tables->del(tableHandle);
        }
        catch (DbRelationError &e) {}
        // throw this exception to display error message in SQL shell
        throw;
    }

    return new QueryResult("Created new table " + table_name);;
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement)
{
    return new QueryResult("Drop not implemented"); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement)
{
    switch(statement->type) {
        case ShowStatement::kTables:
            return show_tables();
            break;
        case ShowStatement::kColumns:
            return show_columns(statement);
            break;
        default:
            throw new SQLExecError("statement not implemented " + statement->type);
    }
}

QueryResult *SQLExec::show_tables()
{
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    ValueDicts *rows = new ValueDicts();
    // tables will only have one column name, which is table_name
    // // attributes will always be TEXT
    SQLExec::tables->get_columns("_tables", *column_names, *column_attributes);
    // use select to get all entries from that table
    Handles *selectResult = SQLExec::tables->select();
    // use project to get all entries from column "table_name"
    for(const Handle handle : *selectResult) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        // "_tables" and "_columns" is in the list too, filter out
        Identifier column_name = row->at("table_name").s;
        if (column_name != Tables::TABLE_NAME && column_name != Columns::TABLE_NAME){
            rows->push_back(row);
        }
    }
    delete selectResult;
    // message should contain the number of records returned.
    string message = "successfully returned " + to_string(rows->size()) + " rows";
    return new QueryResult(column_names, column_attributes, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement)
{
    return new QueryResult("Show columns not implemented"); // FIXME
}
