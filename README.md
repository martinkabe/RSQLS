# RSQLS package
Package for interactive work with SQL Server

# RSQLS - Package for interactive work with SQL Server

**Usage is:**
* pulling data from SQL Server
* pushing data into SQL Server
* retrieving basic info about SQL table
* retrieving basic info about SQL database
* allows user to create table, drop table, delere rows in table or create new table on SQL Server

## Getting Started

*Install package github:*
```
library(devtools)
install_github("martinkabe/RSQLS")
```
*Install package from folder content:*
* download zip file [RSQLS](https://github.com/martinkabe/RSQLS_package/) - Clone or download
```
library(devtools)
install('/RSQLS/package/diR')
library(RSQLS)
?RSQLS # for basic Help
```

### Prerequisites

* Download R version >= R-3.4.2 [RProject](https://www.r-project.org/)
* Install data.table package
```
install.packages("data.table")
```

### Basic functions

**push_data**
* Pushing data into SQL Server.

**pull_data**
* Pulling data from SQL Server.

**send_SQL_task**
* Allows user to create table, drop table, delere rows in table or create new table on SQL Server.

**get_DB_info**
* Retrieving basic info about SQL database. Be sure you have a permissions for access to *sys.dm_db_index_usage_stats*

**get_table_info**
* Retrieving basic info about SQL table.

**Examples**
* push_data
```
push_data(connString, dataFrame, "dbo.TableName")
```
* pull_data
```
pull_data(connString, "SELECT * FROM dbo.TableName")
```

* send_SQL_task
```
send_SQL_task(connString, "CREATE TABLE dbo.TableName (ID int not null, Name varchar(100))")
send_SQL_task(connString, "DELETE FROM dbo.TableName WHERE ColumnName = 'SomeValue'")
send_SQL_task(connString, "DROP TABLE dbo.TableName")
```

* get_DB_info
```
get_DB_info(connString)
```

* get_table_info
*set_connString(datasource, database, usr, pwd)*
```
get_table_info(connString, "dbo.tableName") # If username and password missing or empty Integrated Security=True is used in connection string instead.
```

* How to set up connection string
```
set_connString("LAPTOP-USER\\SQLEXPRESS", "Database_Name")
```

### Performance testing
** Pushing data from data.frame/data.table to table on SQL Server:

| Rows | Columns | DBI-dbWriteTable | RSQL-push_data | RODBC-sqlSave |
| :---: | :---: | :---: | :---: | :---: |
| 1000000 | 6 | 16.42 | 15.94 | 319.10 |
| 5000000 | 6 | 78.69 | 66.23 | 1728.53 |
| 10000000 | 6 | 155.50 | 126.73 | NA |
| 50000000 | 6 | 901.39 | 711.55 | NA |
| 1000000 | 21 | 27.03 | 49.81 | NA |
| 5000000 | 21 | 143.25 | 223.25 | NA |
| 10000000 | 21 | 262.83 | 415.94 | NA |

** Pulling data from table on SQL Server into data.frame/data.table


## Authors

* **Martin Kovarik**


## License

This project is licensed under the GPL-2 | GPL-3.
