# RSQLS package
Package for fast interactive work with SQL Server

# RSQLS - Package for fast interactive work with SQL Server

**Usage is:**
* pulling data from SQL Server
* pushing data into SQL Server
* retrieving basic info about SQL table
* retrieving basic info about SQL database
* allows user to create table, drop table, delere rows in table or create new table on SQL Server

## Getting Started

*Install package directly from github:*
```
library(devtools)
install_github("martinkabe/RSQLS")
```
*Install package from folder content:*
* download zip file [RSQLS](https://github.com/martinkabe/RSQLS/) -> Clone or download -> Download ZIP
```
library(devtools)
install('/RSQLS/package/diR')
library(RSQLS)
?RSQLS # for basic Help
```

### Prerequisites

* .NET Framework 4.5.1 or newer. How do I check it: [link](https://docs.microsoft.com/en-us/dotnet/framework/migration-guide/how-to-determine-which-versions-are-installed/)
* Download R version >= R-3.4.2 [RProject](https://www.r-project.org/)

### Basic functions - description

**push_data**
* Pushing data into SQL Server.
* Table on SQL Server is automatically created if doesn't exit. 
* Data types are automatically estimated (functionality is able to recognize scientific format and convert to appropriate sql data type - int, float, decimal, ... It is also able to distinguish date, datetime format and datetime in ISO format).
```
push_data(connectionString, df, sqltabname, append = FALSE, showprogress = FALSE)
# If append == TRUE then appending new rows into existing SQL table. If append == FALSE then deletes rows in existing SQL table and appends new records.
```

**pull_data**
* Pulling data from SQL Server.
```
pull_data(connectionString, sqltask, showprogress = FALSE)
```

**send_SQL_task**
* Allows user to create table, drop table, delere rows in table or create new table on SQL Server.
```
send_SQL_task(connectionString, sqltask)
```

**get_DB_info**
* Retrieving basic info about SQL database. Be sure you have a permissions for access to *sys.dm_db_index_usage_stats*: check it with *SELECT * FROM sys.dm_db_index_usage_stats*. If not, contact your SQL Server admin.
```
get_DB_info(connectionString)
```

**get_table_info**
* Retrieving basic info about SQL table.
```
get_table_info(connectionString, sqltabname)
```

### Examples
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
```
get_table_info(connString, "dbo.tableName")
```

* How to set up connection string
```
# set_connString(datasource, database, usr, pwd)
# If username and password missing or empty Integrated Security=True is used in connection string instead.
set_connString("LAPTOP-USER\\SQLEXPRESS", "Database_Name")
```

### Performance testing
Tested on Intel(R) Core(TM) i7-7500 CPU, 2.70GHz 2.90GHz, 12GB RAM, x64 Operating System Windows, SQL Server 2014 Express.

* Pushing data from data.frame/data.table to table on SQL Server (average time in seconds after 3 replications) with mixed data types such as int (mixed with scientific notation), varchar, float, date, datetime in ISO format:

| Rows | Columns | DBI::dbWriteTable | RSQLS::push_data | RODBC::sqlSave |
| :---: | :---: | :---: | :---: | :---: |
| 1,000,000 | 6 | 16.42 | 15.94 | 319.10 |
| 5,000,000 | 6 | 78.69 | 66.23 | 1728.53 |
| 10,000,000 | 6 | 155.50 | 126.73 | NA |
| 50,000,000 | 6 | 901.39 | 711.55 | NA |
| 1,000,000 | 21 | 27.03 | 49.81 | NA |
| 5,000,000 | 21 | 143.25 | 223.25 | NA |
| 10,000,000 | 21 | 262.83 | 415.94 | NA |

DBI::dbWriteTable and RODBC::sqlSave incorrectly classified scientific notation (1e5, 1.45e2, ...) as varchar type. The same situation with datetime in ISO format was classified as varchar in both cases. RSQLS::push_data correctly classified scientific notation as int or float and datetime in ISO format is correctly datetime data type.

*Source code for benchmark available at* [link](https://github.com/martinkabe/RSQLS/issues/6)

* Pulling data from table on SQL Server into data.frame/data.table:

*Approximately the same like DBI::dbFetch and many time faster than RODBC::sqlQuery*

## Author

* **Martin Kovarik**


## License

This project is licensed under the GPL-2 | GPL-3.
