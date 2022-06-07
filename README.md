# 5300-Giraffe

_Sprint Verano_

Team:
- Marwa Saleh
- Sonali Desrda

## Milestone 1:

Program written in C++ that runs from the command line and prompts the user for SQL statements and then executes them one at a time, just like the mysql program.

To build the program:
`$ make`

To run the program:
`$ ./sql5300 ~/cpsc5300/data`

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
CREATE TABLE <table_name> (<column_definitions>)
```
* DROP TABLE
#### Syntax:
```
DROP TABLE <table_name>
```
* SHOW TABLES
#### Syntax:
```
SHOW TABLES
```
* SHOW COLUMNS
#### Syntax:
```
SHOW COLUMNS FROM <table_name>
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
DROP INDEX index_name FROM table_name
```
### Usage:
Clean the builds:
<br />`$ make clean`
<br />Build project:
<br />`$ make`
<br />After compiling, run the following command to start the SQL shell:
<br />`$ ./sql5300 [PATH]/data`
<br />To test the storage engine, use the `test` command:
<br />`$ SQL> test`
<br />To exit the SQL shell, use the `quit` command:
<br />`$ SQL> quit`

### Hand-Off

To assist the next team working on project Giraffe, we recorded a walk-through video to show the parts of our code.
You can find watch the [video here](https://seattleu.instructuremedia.com/embed/65dba5e4-cdb8-418c-849f-1829a28759f7).

This is an addendum video to talk about the memory leaks, you may find the [video here](https://seattleu.instructuremedia.com/embed/b18be271-0505-4459-aa9f-f805d33344e2).

---

Sprint Inverno

Team: Jacob Simons
      Merryl Cruz

Milestone 5 / 6:

Basic implementation of b-tree index, missing implementation for delete and range.

##Usage:
Use make to create sql5300
Run with ./sql5300 'database_path'
SQL> test
Test will run premade tests for heaptable and btree implementations.
SQL> quit
Quit will escape the program.
