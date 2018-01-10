# RSQLS_package
Package for interactive work with SQL Server

\name{package}
\alias{package}
\alias{RSQLS}
\title{RSQLS package}
\usage{
\describe{
\item{\link{push_data}}{push_data(connString, dataFrame, "dbo.TableName")}
\item{\link{pull_data}}{pull_data(connString, "SELECT * FROM dbo.TableName")}
\item{\link{send_SQL_task}}{send_SQL_task(connString, "CREATE TABLE dbo.TableName (ID int not null, Name varchar(100))")
send_SQL_task(connString, "DELETE FROM dbo.TableName WHERE ColumnName = 'SomeValue'")
send_SQL_task(connString, "DROP TABLE dbo.TableName")}
\item{\link{get_DB_info}}{get_DB_info(connString)}
\item{\link{get_table_info}}{get_table_info(connString, "dbo.tableName")}
}
}
\description{
Package for interactive work with SQL Server
\describe{
\item{\link{push_data}}{Pushing data into SQL Server.}
\item{\link{pull_data}}{Pulling data from SQL Server.}
\item{\link{send_SQL_task}}{Allows user to create table, drop table, delere rows in table or create new table on SQL Server.}
\item{\link{get_DB_info}}{Retrieving basic info about SQL database.}
\item{\link{get_table_info}}{Retrieving basic info about SQL table.}
}
}
\note{
How to set up SQL Server connection string see \link{set_connString}.
}
\author{
Martin Kovarik
}
