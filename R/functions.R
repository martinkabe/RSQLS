
# RSQLS functions - SQL part -------------------------------------------

#' Create connection string from function parameters
#'
#' This function defines SQL Server connection string
#' from function parameters.
#' @param datasource Server name
#' @param database Database name
#' @param usr Username
#' @param pwd Password
#' @note If username and password missing or empty \strong{Integrated Security=True} is used in connection string instead.
#' @return Connection string
#' @export
#' @examples
#' set_connString("LAPTOP-USER\\SQLEXPRESS", "Database_Name")
set_connString <- function(datasource, database, usr, pwd) {
  ds <- paste("Data Source=", datasource, ";", sep = "")
  db <- paste("Initial Catalog=", database, ";", sep = "")
  if ((missing(usr) & missing(pwd)) || (usr=="" & pwd=="")) {
    last_param <- "Integrated Security=True;MultipleActiveResultSets=True;"
  } else {
    last_param <- paste("Integrated Security=False;", "User Id=", usr, ";", "Password=", pwd, ";", "MultipleActiveResultSets=True;", sep = "")
  }
  return(paste('"', ds, db, last_param, '"', sep = ""))
}

replace_spaced_words <- function(str_string) {
  complete_string <- ""
  str_text <- strsplit(str_string,"\\\\")
  for(i in 1:length(str_text[[1]])) {
    num_words <- length(strsplit(str_text[[1]][[i]], " ")[[1]])
    # print(num_words)
    if(num_words > 1) {
      # this string should be double quoted
      complete_string <- paste(complete_string, '"', str_text[[1]][[i]], '"', "\\", sep="")
    } else {
      complete_string <- paste(complete_string, str_text[[1]][[i]], "\\", sep="")
    }
  }
  return(complete_string)
}


# Push data into SQL table ------------------------------------------------

#' Pushing data from data.frame object into SQL table on SQL Server
#'
#' This function pushes data from data.frame object
#' into SQL table on SQL server.
#' @param connectionString SQL connection string
#' @param df Data.Frame to be pushed into SQL table
#' @param sqltabname SQL table name
#' @param append Append new rows (If \strong{append == TRUE} then appending new rows into existing SQL table. If \strong{append == FALSE} then deletes rows in existing SQL table and appends new records.)
#' @param showprogress Showing progress (default value is FALSE)
#' @note Table is automatically created if doesn't exist on SQL Server with automatically identified data types.
#' @export
#' @examples
#' push_data(connString, dataFrame, "dbo.TableName")
#' @note How to set up SQL Server connection string see \link{set_connString}.
push_data <- function(connectionString
                      ,df
                      ,sqltabname
                      ,append = FALSE
                      ,showprogress = FALSE) {
  options(scipen=999)
  # df_name <- paste('', deparse(substitute(df)), '', sep = "")
  # if (!exists(df_name) || !is.data.frame(get(df_name))) {
  #   print(paste("data.frame called ", deparse(substitute(df)), " does not exist!", sep = ""))
  #   return("Try it again")
  # }
  if (missing(connectionString)) {
    print("Connection string is missing!")
    return("Try it again")
  }
  pathtocsvloader <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Loader/csv_to_sql_loader.exe", sep = ""))
  pathtocsvloader <- replace_spaced_words(pathtocsvloader)
  pathtocsvloader <- gsub('.{1}$', '', pathtocsvloader)
  # logic for pathtocsvfiles variable
  pathtocsvfiles <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/data/", sep = ""))
  if (!endsWith(pathtocsvfiles, "\\")) {
    pathtocsvfiles <- paste(pathtocsvfiles,"\\", sep = "")
  }

  if (missing(sqltabname)) {
    print("SQL table name is missing!")
    return("Try it again")
  }
  if (dim(df)[1] < 1) {
    print(paste("Data Frame has ", dim(df)[1], " rows and ", dim(df)[2], " columns.", " No inserted record into table ", sqltabname, sep = ""))
  } else {
    print(paste("Data Frame has ", dim(df)[1], " rows and ", dim(df)[2], " columns.", sep = ""))
    # sqltabname <- gsub("\\[|\\]", "", sqltabname)
    if (length(strsplit(sqltabname,"\\.")[[1]]) > 1) {
      sqltabname_prev <- gsub("\\[|\\]", "", gsub("^[^.]*.", "", sqltabname))
    } else {
      sqltabname_prev <- sqltabname
    }
    data.table::fwrite(df, paste(pathtocsvfiles,"\\", paste(sqltabname_prev,".csv", sep = ""), sep = ""), row.names = FALSE, sep = "\t")
    sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
    if (append == FALSE) append = 1 else append = 0
    delete_tab <- paste('"', append, '"', sep = "")
    operation <- paste('"push"', sep = "")
    if (showprogress == FALSE) showprogress = 0 else showprogress = 1
    show_progress <- paste('"', showprogress, '"', sep = "")
    real_pathtocsvfile <- paste('"', pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""),'"', sep = "")
    file_to_be_deleted <- paste(pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""), sep = "")
    ss <- paste('', pathtocsvloader, " ", connectionString, " ", real_pathtocsvfile, " ", sql_tab_name, " ", operation, " ", delete_tab, " ", show_progress, sep = "")
    # Call shell command
    shell(ss)
    # Delete csv file
    if (file.exists(file_to_be_deleted)) invisible(file.remove(file_to_be_deleted)) else return("Try it again")
  }
}

# Pull data from SQL table ------------------------------------------------

#' Pull data from SQL server via SQL query
#'
#' This function pulls the data from SQL server
#' via SQL query and returns data.table and data.frame object
#' @param connectionString Connection string to SQL server
#' @param sqltask SQL query for selecting data on SQL server
#' @param showprogress Showing progress (default value is FALSE)
#' @return Returns data.frame and data.table
#' @export
#' @examples
#' pull_data(connString, "SELECT * FROM dbo.TableName")
#' @note How to set up SQL Server connection string see \link{set_connString}.
pull_data <- function(connectionString
                      ,sqltask
                      ,showprogress = FALSE) {
  options(scipen=999)
  if (missing(connectionString)) {
    print("Connection string is missing!")
    return("Try it again")
  }
  pathtocsvloader <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Loader/csv_to_sql_loader.exe", sep = ""))
  pathtocsvloader <- replace_spaced_words(pathtocsvloader)
  pathtocsvloader <- gsub('.{1}$', '', pathtocsvloader)
  # logic for pathtocsvfiles variable
  pathtocsvfiles <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Data/", sep = ""))
  if (!endsWith(pathtocsvfiles, "\\")) {
    pathtocsvfiles <- paste(pathtocsvfiles,"\\", sep = "")
  }
  sqltabname <- "tempTable"
  sqltabname <- gsub("\\[|\\]", "", sqltabname)
  if (length(strsplit(sqltabname,"\\.")[[1]]) > 1) {
    sqltabname_prev <- gsub("^[^.]*.", "", sqltabname)
  } else {
    sqltabname_prev <- sqltabname
  }
  sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
  sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
  operation <- paste('"pull"', sep = "")
  if (missing(sqltask)) {
    print("SQL task shouldn't be missing!")
    return("Try it again")
  }
  sql_task <- paste('"', sqltask, '"', sep = "")
  sql_task <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", gsub("[\r\n]", "", sql_task), perl=TRUE)
  if (showprogress == FALSE) showprogress = 0 else showprogress = 1
  show_progress <- paste('"', showprogress, '"', sep = "")
  real_pathtocsvfile <- paste('"', pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""),'"', sep = "")
  file_to_be_deleted <- paste(pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""), sep = "")
  ss <- paste('', pathtocsvloader, " ", connectionString, " ", real_pathtocsvfile, " ", "null", " ", operation, " ", sql_task, " ", show_progress, sep = "")
  # Call shell command
  shell(ss)
  if (file.exists(file_to_be_deleted)) out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE) else return("Try it again")
  # Delete csv file
  if (file.exists(file_to_be_deleted)) invisible(file.remove(file_to_be_deleted)) else return("Try it again")
  return(out)
}

# Drop, Delete, Create table ----------------------------------------------

#' Drop, delete or create table
#'
#' This function allows user to drop table, delete rows in table
#' or create new table on SQL Server
#' @param connectionString Connection string to SQL server
#' @param sqltask SQL query for retrieving data from SQL server
#' @export
#' @examples
#' send_SQL_task(connString, "CREATE TABLE dbo.TableName (ID int not null, Name varchar(100))")
#' send_SQL_task(connString, "DELETE FROM dbo.TableName WHERE ColumnName = 'SomeValue'")
#' send_SQL_task(connString, "DROP TABLE dbo.TableName")
#' @note How to set up SQL Server connection string see \link{set_connString}.
send_SQL_task <- function(connectionString
                          ,sqltask)
{
  if (missing(connectionString)) {
    print("Connection string is missing!")
    return("Try it again")
  }
  pathtocsvloader <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Loader/csv_to_sql_loader.exe", sep = ""))
  pathtocsvloader <- replace_spaced_words(pathtocsvloader)
  pathtocsvloader <- gsub('.{1}$', '', pathtocsvloader)
  if (missing(sqltask)) {
    print("SQL task shouldn't be missing!")
    return("Try it again")
  }
  # logic for pathtocsvfiles variable
  sql_task <- paste('"', sqltask, '"', sep = "")
  sql_task <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", gsub("[\r\n]", "", sql_task), perl=TRUE)
  ss <- paste('', pathtocsvloader, " ", connectionString, " ", sql_task, sep = "")
  shell(ss)
}

# Get DB info -----------------------------------------------------------

#' Get database info
#'
#' This function retrieves basic info about database defined
#' in SQL Server connection string.
#' @param connectionString Connection string to SQL server
#' @return Returns data.frame and data.table
#' @export
#' @examples
#' get_DB_info(connectionString)
#' @note How to set up SQL Server connection string see \link{set_connString}.
#' @note Be sure you have a permissions for access to sys.dm_db_index_usage_stats: check it with SELECT * FROM sys.dm_db_index_usage_stats. If not, contact your SQL Server admin.
get_DB_info <- function(connectionString) {
  options(scipen=999)
  if (missing(connectionString)) {
    print("Connection string is missing!")
    return("Try it again")
  }
  pathtocsvloader <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Loader/csv_to_sql_loader.exe", sep = ""))
  pathtocsvloader <- replace_spaced_words(pathtocsvloader)
  pathtocsvloader <- gsub('.{1}$', '', pathtocsvloader)
  # logic for pathtocsvfiles variable
  pathtocsvfiles <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Data/", sep = ""))
  if (!endsWith(pathtocsvfiles, "\\")) {
    pathtocsvfiles <- paste(pathtocsvfiles,"\\", sep = "")
  }
  sqltabname <- "tempDBInfo"
  sqltabname <- gsub("\\[|\\]", "", sqltabname)
  if (length(strsplit(sqltabname,"\\.")[[1]]) > 1) {
    sqltabname_prev <- gsub("^[^.]*.", "", sqltabname)
  } else {
    sqltabname_prev <- sqltabname
  }
  sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
  sql_task <- paste('"dbinfo"', sep = "")
  real_pathtocsvfile <- paste('"', pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""),'"', sep = "")
  file_to_be_deleted <- paste(pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""), sep = "")
  ss <- paste('', pathtocsvloader, " ", connectionString, " ", sql_task, " ", real_pathtocsvfile, " ", "null", sep = "")
  # Call shell command
  shell(ss)
  if (file.exists(file_to_be_deleted)) out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE) else return("Try it again")
  # Delete csv file
  if (file.exists(file_to_be_deleted)) invisible(file.remove(file_to_be_deleted)) else return("Try it again")
  return(out)
}

# Get table info ----------------------------------------------------------

#' Get table info
#'
#' This function retrieves basic info about table
#' @param connectionString Connection string to SQL server
#' @param sqltabname SQL table name
#' @return Returns data.frame and data.table
#' @export
#' @examples
#' get_table_info(connString, "dbo.tableName")
#' @note How to set up SQL Server connection string see \link{set_connString}.
get_table_info <- function(connectionString
                           ,sqltabname) {
  options(scipen=999)
  if (missing(connectionString)) {
    print("Connection string is missing!")
    return("Try it again")
  }
  pathtocsvloader <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Loader/csv_to_sql_loader.exe", sep = ""))
  pathtocsvloader <- replace_spaced_words(pathtocsvloader)
  pathtocsvloader <- gsub('.{1}$', '', pathtocsvloader)
  # logic for pathtocsvfiles variable
  pathtocsvfiles <- gsub("/","\\\\",paste(system.file(package = "RSQLS")[1],"/Data/", sep = ""))
  if (!endsWith(pathtocsvfiles, "\\")) {
    pathtocsvfiles <- paste(pathtocsvfiles,"\\", sep = "")
  }
  if (missing(sqltabname)) {
    print("SQL table name is missing!")
    return("Try it again")
  }
  # sqltabname <- gsub("\\[|\\]", "", sqltabname)
  if (length(strsplit(sqltabname,"\\.")[[1]]) > 1) {
    sqltabname_prev <- gsub("\\[|\\]", "", gsub("^[^.]*.", "", sqltabname))
  } else {
    sqltabname_prev <- sqltabname
  }
  sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
  sql_task <- paste('"tableinfo"', sep = "")
  real_pathtocsvfile <- paste('"', pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""),'"', sep = "")
  file_to_be_deleted <- paste(pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""), sep = "")
  ss <- paste('', pathtocsvloader, " ", connectionString, " ", sql_task, " ", real_pathtocsvfile, " ", sql_tab_name, sep = "")
  # Call shell command
  shell(ss)
  if (file.exists(file_to_be_deleted)) out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE) else return("Try it again")
  # Delete csv file
  if (file.exists(file_to_be_deleted)) invisible(file.remove(file_to_be_deleted)) else return("Try it again")
  return(out)
}

# Package Info ------------------------------------------------------------

#' @name package
#' @aliases RSQLS
#' @title RSQLS package
#' @description Package for interactive work with SQL Server
#' \describe{
#' \item{\link{push_data}}{Pushing data into SQL Server.}
#' \item{\link{pull_data}}{Pulling data from SQL Server.}
#' \item{\link{send_SQL_task}}{Allows user to create table, drop table, delere rows in table or create new table on SQL Server.}
#' \item{\link{get_DB_info}}{Retrieving basic info about SQL database.}
#' \item{\link{get_table_info}}{Retrieving basic info about SQL table.}
#' }
#' @author Martin Kovarik
#' @usage
#' \describe{
#' \item{\link{push_data}}{push_data(connString, dataFrame, "dbo.TableName")}
#' \item{\link{pull_data}}{pull_data(connString, "SELECT * FROM dbo.TableName")}
#' \item{\link{send_SQL_task}}{send_SQL_task(connString, "CREATE TABLE dbo.TableName (ID int not null, Name varchar(100))")
#' send_SQL_task(connString, "DELETE FROM dbo.TableName WHERE ColumnName = 'SomeValue'")
#' send_SQL_task(connString, "DROP TABLE dbo.TableName")}
#' \item{\link{get_DB_info}}{get_DB_info(connString)}
#' \item{\link{get_table_info}}{get_table_info(connString, "dbo.tableName")}
#' }
#' @note How to set up SQL Server connection string see \link{set_connString}.
#'
RSQLS <- function(){}
