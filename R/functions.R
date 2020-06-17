
# RSQLS functions - SQL part -------------------------------------------

#' Create connection string from function parameters.
#'
#' This function defines SQL Server connection string.
#' from function parameters.
#' @param datasource Server name.
#' @param database Database name.
#' @param usr User name for authentication.
#' @param pwd Password for authentication.
#' @note If username and password missing or empty \strong{Integrated Security=True} is used in connection string instead.
#' @return SQL Server connection string.
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

convert_date_format_toString <- function(df) {
  # define ingerits for date formats
  is.POSIXct <- function(x) inherits(x, "POSIXct")
  is.POSIXlt <- function(x) inherits(x, "POSIXlt")
  is.POSIXt <- function(x) inherits(x, "POSIXt")
  # extract column numbers
  col_nums <- c()
  col_nums <- c(col_nums, which(sapply(df, is.POSIXct)))
  col_nums <- c(col_nums, which(sapply(df, is.POSIXlt)))
  col_nums <- c(col_nums, which(sapply(df, is.POSIXt)))
  col_nums <- sort(unique(col_nums))
  df[col_nums] <- sapply(df[col_nums], as.character)
  return(df)
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

#' Pushing data from data.frame object into SQL table on SQL Server.
#'
#' This function pushes data from data.frame object
#' into SQL table on SQL server.
#' @param connectionString SQL connection string.
#' @param df Data.Frame to be pushed into SQL table.
#' @param sqltabname SQL table name.
#' @param append Append new rows (If \strong{append == TRUE} then appending new rows into existing SQL table. If \strong{append == FALSE} then deletes rows in existing SQL table and appends new records. Default value is set to \strong{TRUE}).
#' @param showprogress Showing progress (default value is set to \strong{FALSE}).
#' @param quotes When \strong{"auto"}, character fields, factor fields and column names will only be surrounded by double quotes when they need to be; i.e., when the field contains the separator sep, a line ending \\n, the double quote itself. If \strong{FALSE} the fields are not wrapped with quotes even if this would break the CSV due to the contents of the field. If \strong{TRUE} double quotes are always included other than around numeric fields, as write.csv. Default value is set to \strong{"auto"}.
#' @param separator Default is \strong{"|"}. This determines what separator is used during csv is generated.
#' @note Table is automatically created if doesn't exist on SQL Server with automatically identified data types.
#' @export
#' @examples
#' \dontrun{
#' push_data(connectionString, dataFrame, "dbo.TableName")
#' }
#' @note How to set up SQL Server connection string see \link{set_connString}.
push_data <- function(connectionString
                      ,df
                      ,sqltabname
                      ,append = TRUE
                      ,showprogress = FALSE
                      ,quotes = "auto"
                      ,separator = "|") {
  options(scipen=999)
  # df_name <- paste('', deparse(substitute(df)), '', sep = "")
  # if (!exists(df_name) || !is.data.frame(get(df_name))) {
  #   print(paste("data.frame called ", deparse(substitute(df)), " does not exist!", sep = ""))
  #   return("Try it again")
  # }
  # Convert datetime or date to string format
  df <- convert_date_format_toString(df)
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
    if (showprogress) {
      print(paste("Data Frame has ", dim(df)[1], " rows and ", dim(df)[2], " columns.", " No inserted record into table ", sqltabname, sep = ""))
    }
  } else {
    if (showprogress) {
      print(paste("Data Frame has ", dim(df)[1], " rows and ", dim(df)[2], " columns.", sep = ""))
    }
    # sqltabname <- gsub("\\[|\\]", "", sqltabname)
    if (length(strsplit(sqltabname,"\\.")[[1]]) > 1) {
      sqltabname_prev <- gsub("\\[|\\]", "", gsub("^[^.]*.", "", sqltabname))
    } else {
      sqltabname_prev <- sqltabname
    }
    data.table::fwrite(df, paste(pathtocsvfiles,"\\", paste(sqltabname_prev,".csv", sep = ""), sep = ""), row.names = FALSE, sep = separator, quote = quotes)
    sql_tab_name <- paste('"', sqltabname, '"', sep = "")
    if (append == FALSE) append = 1 else append = 0
    delete_tab <- paste('"', append, '"', sep = "")
    operation <- paste('"push"', sep = "")
    if (showprogress == FALSE) showprogress = 0 else showprogress = 1
    show_progress <- paste('"', showprogress, '"', sep = "")
    separator <- paste('"', separator, '"', sep = "")
    real_pathtocsvfile <- paste('"', pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""),'"', sep = "")
    file_to_be_deleted <- paste(pathtocsvfiles, paste(sqltabname_prev, ".csv", sep = ""), sep = "")
    ss <- paste('', pathtocsvloader, " ", connectionString, " ", real_pathtocsvfile, " ", sql_tab_name, " ", operation, " ", delete_tab, " ", show_progress, " ", separator, sep = "")
    # Call shell command
    oldw <- getOption("warn")
    options(warn = -1)
    sc <- shell(ss)
    # Delete csv file
    if (file.exists(file_to_be_deleted)){
      invisible(file.remove(file_to_be_deleted))
    } else{
      options(warn = oldw)
      stop('See the previous messages for more details.')
    }
    if( sc == 1 ) {
      options(warn = oldw)
      stop('See the previous messages for more details.')
    } else {
      options(warn = oldw)
    }
  }
}

# Pull data from SQL table ------------------------------------------------

#' Pull data from SQL server via SQL query
#'
#' This function pulls the data from SQL server
#' via SQL query and returns data.table and data.frame object
#' @param connectionString Connection string to SQL server.
#' @param sqltask SQL query for selecting data on SQL server.
#' @param showprogress Showing progress (default value is set to \strong{FALSE}).
#' @return Returns data.frame and data.table object.
#' @export
#' @examples
#' \dontrun{
#' pull_data(connectionString, "SELECT * FROM dbo.TableName")
#' }
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
  sqltabname <- paste("tempTable", sample(x = 100000, size = 1, replace = F), sep = "")
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
  oldw <- getOption("warn")
  options(warn = -1)
  sc <- shell(ss)
  if (file.exists(file_to_be_deleted)){
    out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE)
  } else{
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  # Delete csv file
  if (file.exists(file_to_be_deleted)) {
    invisible(file.remove(file_to_be_deleted))
    options(warn = oldw)
  } else {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  if( sc == 1 ) {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  } else {
    options(warn = oldw)
  }
  return(out)
}

# dPull data from SQL table ------------------------------------------------

#' Direct pull data from SQL server via SQL query into flat file with StreamReader class.
#'
#' This function pulls the data from SQL server directly into flat file via StreamReader class
#' via SQL query and returns data.table and data.frame object.
#' @param connectionString Connection string to SQL server.
#' @param sqltask SQL query for selecting data on SQL server.
#' @param showprogress Showing progress (default value is \strong{FALSE}).
#' @return Returns data.frame and data.table object.
#' @export
#' @examples
#' \dontrun{
#' dpull_data(connectionString, "SELECT * FROM dbo.TableName")
#' }
#' @note How to set up SQL Server connection string see \link{set_connString}.
dpull_data <- function(connectionString
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
  sqltabname <- paste("tempTable", sample(x = 100000, size = 1, replace = F), sep = "")
  sqltabname <- gsub("\\[|\\]", "", sqltabname)
  if (length(strsplit(sqltabname,"\\.")[[1]]) > 1) {
    sqltabname_prev <- gsub("^[^.]*.", "", sqltabname)
  } else {
    sqltabname_prev <- sqltabname
  }
  sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
  sql_tab_name <- paste('"', sqltabname, '"', sep = "") # '"dbo.CFTC_Disaggregated_Raw_test"'
  operation <- paste('"dpull"', sep = "")
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
  oldw <- getOption("warn")
  options(warn = -1)
  sc <- shell(ss)
  if (file.exists(file_to_be_deleted)){
    out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE)
  } else{
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  # Delete csv file
  if (file.exists(file_to_be_deleted)) {
    invisible(file.remove(file_to_be_deleted))
    options(warn = oldw)
  } else {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  if( sc == 1 ) {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  } else {
    options(warn = oldw)
  }
  return(out)
}

# Drop, Delete, Create table ----------------------------------------------

#' Drop, delete or create table
#'
#' This function allows user to drop table, delete rows in table
#' or create new table on SQL Server.
#' @param connectionString Connection string to SQL server.
#' @param sqltask SQL query for retrieving data from SQL server.
#' @export
#' @examples
#' \dontrun{
#' send_SQL_task(connectionString, "CREATE TABLE dbo.TableName (ID int not null, Name varchar(100))")
#' send_SQL_task(connectionString, "DELETE FROM dbo.TableName WHERE ColumnName = 'SomeValue'")
#' send_SQL_task(connectionString, "DROP TABLE dbo.TableName")
#' }
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
  # Call shell command
  oldw <- getOption("warn")
  options(warn = -1)
  sc <- shell(ss)
  if( sc == 1 ) {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  } else {
    options(warn = oldw)
  }
}

# Get DB info -----------------------------------------------------------

#' Get database info
#'
#' This function retrieves basic info about database defined
#' in SQL Server connection string.
#' @param connectionString Connection string to SQL server.
#' @return Returns data.frame and data.table object.
#' @export
#' @examples
#' \dontrun{
#' get_DB_info(connectionString)
#' }
#' @note How to set up SQL Server connection string see \link{set_connString}. Be also sure you have a permissions for access to sys.dm_db_index_usage_stats:
#' check it with SELECT * FROM sys.dm_db_index_usage_stats. If not, contact your SQL Server admin.

get_DB_info <- function(connectionString) {
  options(scipen=999)
  if (missing(connectionString)) {
    print("Connection string is missing!")
    return("Try it again")
  }
  # connectionString <- connectionString
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
  oldw <- getOption("warn")
  options(warn = -1)
  sc <- shell(ss)
  if (file.exists(file_to_be_deleted)){
    out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE)
  } else{
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  # Delete csv file
  if (file.exists(file_to_be_deleted)){
    invisible(file.remove(file_to_be_deleted))
  } else{
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  if( sc == 1 ) {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  } else {
    options(warn = oldw)
  }
  return(out)
}

# Get table info ----------------------------------------------------------

#' Get table info
#'
#' This function retrieves basic info about table.
#' @param connectionString Connection string to SQL server.
#' @param sqltabname SQL table name.
#' @return Returns data.frame and data.table object.
#' @export
#' @examples
#' \dontrun{
#' get_table_info(connectionString, "dbo.tableName")
#' }
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
  oldw <- getOption("warn")
  options(warn = -1)
  sc <- shell(ss)
  if (file.exists(file_to_be_deleted)){
    out <- data.table::fread(file_to_be_deleted, stringsAsFactors = FALSE, sep = "~", fill = TRUE)
  } else{
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  # Delete csv file
  if (file.exists(file_to_be_deleted)){
    invisible(file.remove(file_to_be_deleted))
  } else{
    options(warn = oldw)
    stop('See the previous messages for more details.')
  }
  if( sc == 1 ) {
    options(warn = oldw)
    stop('See the previous messages for more details.')
  } else {
    options(warn = oldw)
  }
  return(out)
}

# Package Info ------------------------------------------------------------

#' @name package
#' @aliases RSQLS
#' @title RSQLS package
#' @description Package for interactive work with SQL Server.
#' \describe{
#' \item{\link{push_data}}{Pushing data into SQL Server.}
#' \item{\link{pull_data}}{Pulling data from SQL Server into StringBuilder and then into flat file.}
#' \item{\link{dpull_data}}{Pulling data from SQL Server directly into flat file via StreamReader class.}
#' \item{\link{send_SQL_task}}{Allows user to create table, drop table, delere rows in table or create new table on SQL Server.}
#' \item{\link{get_DB_info}}{Retrieving basic info about SQL database.}
#' \item{\link{get_table_info}}{Retrieving basic info about SQL table.}
#' }
#' @usage
#' push_data(connectionString, DataFrame, "dbo.TableName", append = F, showprogress = F)
#' pull_data(connectionString, "SELECT * FROM dbo.TableName")
#' dpull_data(connectionString, "SELECT * FROM dbo.TableName")
#' send_SQL_task(connectionString, "CREATE TABLE dbo.TableName (ID int not null, Name varchar(100))")
#' send_SQL_task(connectionString, "DELETE FROM dbo.TableName WHERE ColumnName = 'SomeValue'")
#' send_SQL_task(connectionString, "DROP TABLE dbo.TableName")
#' get_DB_info(connectionString)
#' get_table_info(connectionString, "dbo.tableName")
#' @author Martin Kovarik
#' @note How to set up SQL Server connection string see \link{set_connString}.
#' @importFrom data.table fread fwrite
#' @importFrom "utils" "data"
#'
RSQLS <- function(){}
