# 5300-Giraffe

_Sprint Verano_

Team:
- Marwa Saleh
- Sonali Desrda

## Milestone 1:

Program written in C++ that runs from the command line and prompts the user for SQL statements and then executes them one at a time, just like the mysql program.

To build the program:
$ make

To run the program:
$ ./sql5300 ~/cpsc5300/data

## Milestone 2:

The storage engine is made up of three layers: DbBlock, DbFile, and DbRelation.
Created a program for the implementations for the Heap Storage Engine's version of each: SlottedPage, HeapFile, and HeapTable.

Hand off:
The video is added in a zip file in the project

---

_Sprint Otoño_

Team:
- Ana Mendes
- Keerthana Thonupunuri

## Milestone 3:

Rudimentary implementation of a Schema Storage that support the following commands:
* CREATE TABLE
#### Syntax:
```
<table_definition> ::= CREATE TABLE <table_name> (<column_definitions> )
<column_definitions> ::= <column_definition> | <column_definition>, <column_definitions>
<column_definition> ::= <column_name> <datatype>
```
* DROP TABLE
#### Syntax:
```
<drop_table_statement> ::= DROP TABLE <table_name>
```
* SHOW TABLES
#### Syntax:
```
<show_tables_statement> ::= SHOW TABLES
```
* SHOW COLUMNS
#### Syntax:
```
<show_columns_statement> ::= SHOW COLUMNS FROM <table_name>
```

## Milestone 4:

Rudimentary implementation of SQL index commands. Supports the following commands:
* CREATE INDEX
#### Syntax:
```
CREATE INDEX index_name ON table_name [USING {BTREE | HASH}] (col1, col2, ...)
```
* SHOW INDEX
#### Syntax:
```
SHOW INDEX FROM table_name
```
* DROP INDEX
#### Syntax:
```
DROP INDEX index_name ON table_name
```
