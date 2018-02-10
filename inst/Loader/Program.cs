using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace csv_to_sql_loader
{
    class Program
    {
        static void Main(string[] args)
        {
            // Test connection:
            Tuple<bool, string> isConnected = Functions.IsServerConnected(args[0]);
            if (!isConnected.Item1)
            {
                Console.WriteLine(isConnected.Item2);
                Environment.Exit(0);
            }

            Console.WriteLine();
            bool newtable = false;

            if (args.Length == 2)
            {
                // SELECT is not allowed for this type of function
                if (args[1].ToLower().Contains("select")) { Console.WriteLine("You can CREATE, DELETE or DROP table.\nIf you want return something from DB, use parametrized version: pull_data(connectionString = , sqltask = , showprogress = FALSE)!\nPlease, see the documentation!"); }
                Functions.SQLQueryTask(args[1], args[0]);
                Environment.Exit(0);
            }

            else if (args.Length == 4)
            {
                Stopwatch sqltask = new Stopwatch();
                sqltask.Start();

                // SELECT is not allowed for this type of function
                if (args[1].ToLower().Contains("select")) { Console.WriteLine("You can CREATE, DELETE or DROP table.\nIf you want return something from DB, use parametrized version: pull_data(connectionString = , sqltask = , showprogress = FALSE)!\nPlease, see the documentation!"); }

                // Implements delete table function
                if (args[1].ToLower() == "deletetable")
                {
                    Functions.DropTable(args[3], args[0]);
                    Console.WriteLine("Table " + args[3] + " has been deleted.");
                    Environment.Exit(0);
                }
                // Implements table info function
                else if (args[1].ToLower() == "tableinfo")
                {
                    if (!Functions.IfSQLTableExists(args[3], args[0])) { Console.WriteLine("Table " + args[3] + " doesn't exist."); Environment.Exit(0); }
                    string tableinfo_sql = @"SELECT [Column Name],[Data type],[Max Length],[precision],[scale],[is_nullable],[Primary Key]
                                        FROM
                                        (
                                        SELECT [Column Name],[Data type],[Max Length],[precision],[scale],[is_nullable],[Primary Key],
                                        r_number, ROW_NUMBER() OVER(PARTITION BY [Column Name] ORDER BY [Primary Key] DESC) rn
                                        FROM
                                        (
                                        SELECT
                                            c.name 'Column Name',
                                            t.Name 'Data type',
                                            c.max_length 'Max Length',
                                            c.[precision],
                                            c.[scale],
                                            c.is_nullable,
                                            ISNULL(i.is_primary_key, 0) 'Primary Key',
	                                        ROW_NUMBER() over(ORDER BY (SELECT NULL)) r_number
                                        FROM    
                                            sys.columns c
                                        INNER JOIN 
                                            sys.types t ON c.user_type_id = t.user_type_id
                                        LEFT OUTER JOIN 
                                            sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                                        LEFT OUTER JOIN 
                                            sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                                        WHERE
                                            c.object_id = OBJECT_ID('" + args[3] + "')) a ) b WHERE b.rn = 1 ORDER BY b.r_number";

                    Functions.WriteFromDBToCSV(tableinfo_sql, args[2], false, args[0]);
                    Console.WriteLine("Basic info about table " + args[3] + " has been created.");
                    Environment.Exit(0);
                }
                // Implements db info function
                else if (args[1].ToLower() == "dbinfo")
                {
                    string dbinfo_sql = @"select distinct entireView.*,
                                    case
	                                    when sys_stats.[last_user_update] is null
		                                    then st.[modify_date]
	                                    else sys_stats.[last_user_update]
                                    end as [LAST_MODIFIED]
                                    from
                                    (
                                    select origTab.[TABLE_SCHEMA],origTab.[TABLE_NAME],origTab.[TABLE_TYPE],origTab.[TABLE_CATALOG],origTab.[ROWS_COUNT],colTab.[COLS_COUNT]
                                    from
                                    (
                                    select isc.[TABLE_NAME], count(distinct [COLUMN_NAME]) [COLS_COUNT],
                                    'BASE TABLE' [TABLE_TYPE]
                                    from INFORMATION_SCHEMA.COLUMNS isc
                                    group by isc.[TABLE_NAME]
                                    ) colTab
                                    right join
                                    (
                                    select [TABLE_SCHEMA], tbl.name as [TABLE_NAME],[TABLE_TYPE],[TABLE_CATALOG],[rows] as [ROWS_COUNT] from
                                    (
                                    select name,id,ist.TABLE_TYPE,ist.TABLE_CATALOG,ist.TABLE_SCHEMA from
                                    (
                                    select name, id, case xtype
	                                    when 'U' then 'BASE TABLE'
	                                    when 'V' then 'VIEW'
	                                    end as [TABLE_TYPE]
                                    from sysobjects
                                    ) so
                                    right join INFORMATION_SCHEMA.TABLES ist
                                    on so.[name]=ist.[TABLE_NAME] and so.[TABLE_TYPE]=ist.[TABLE_TYPE]
                                    ) as tbl
                                    left join sysindexes si on tbl.id=si.id
                                    where si.indid in (0,1) or si.indid is null
                                    ) origTab
                                    on origTab.[TABLE_NAME]=colTab.[TABLE_NAME] and origTab.[TABLE_TYPE]=colTab.[TABLE_TYPE]
                                    ) as entireView
                                    left join sys.tables st
                                    on entireView.[TABLE_NAME]=st.name
                                    left join	(
				                                    select OBJECT_NAME(OBJECT_ID) AS TableName, [last_user_update]
				                                    from sys.dm_db_index_usage_stats
				                                    WHERE database_id = DB_ID(db_name())
			                                    ) as sys_stats on
			                                    entireView.[TABLE_NAME]=sys_stats.TableName
                                    order by entireView.[ROWS_COUNT] desc";
                    Functions.WriteFromDBToCSV(dbinfo_sql, args[2], false, args[0]);
                    Console.WriteLine("Basic info about database has been created.");
                    Environment.Exit(0);
                }
                // Another SQL query task
                else
                {
                    Console.WriteLine("Not sure what you are trying to achieve... Please, read the documentation!");
                    Environment.Exit(0);
                }
                
                sqltask.Stop();
                Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMili seconds: {2}",
                    sqltask.Elapsed.Minutes, sqltask.Elapsed.Seconds, sqltask.Elapsed.TotalMilliseconds);

                Environment.Exit(0);
            }
            else if (args.Length == 6)
            {
                string connectionString = args[0]; // "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=Data;Integrated Security=True;"
                string csvFilePath = args[1]; // "C:\\pathToFile\\data.csv"
                string tableName = args[2]; // "dbo.DataTable"
                                            // push: push, delete tab, show progress
                                            // pull: pull, sql query, show progress
                string push_or_pull_1 = args[3]; // "push"
                string push_or_pull_2 = args[4]; // "1"
                string push_or_pull_3 = args[5]; // "1"

                #region Input validation
                // Validate inputs:
                // 1) Check array
                // push: push, delete tab, show progress
                // pull: pull, sql query, show progress
                try
                {
                    //foreach (var item in args)
                    //{
                    //    if (item == null | item == string.Empty | item == "") { Console.WriteLine("No of arguments shouldn't be empty!"); Environment.Exit(0); }
                    //}

                    if (push_or_pull_1.ToLower() == "push")
                    {
                        if (args.Length != 6)
                        {
                            Console.WriteLine("Incorrect no of arguments: csvFilePath, tableName, push, remove tab (0/1), show progress (0/1)!");
                            Environment.Exit(0);
                        }
                        // handle for remove tab and show progress
                        string[] possible_vals = { "0", "1" };
                        if (!possible_vals.Contains(push_or_pull_2) | !possible_vals.Contains(push_or_pull_3))
                        {
                            Console.WriteLine("Remove tab and show progress arguments should be 1 or 0!");
                            Environment.Exit(0);
                        }
                    }
                    else if (push_or_pull_1.ToLower() == "pull")
                    {
                        if (args.Length != 6)
                        {
                            Console.WriteLine("Incorrect no of arguments: csvFilePath, tableName, pull, sql query, show progress (0/1)!");
                            Environment.Exit(0);
                        }
                        // handle sql query
                        if (push_or_pull_2.ToLower().Contains("delete") |
                            push_or_pull_2.ToLower().Contains("drop") |
                            push_or_pull_2.ToLower().Contains("insert") |
                            push_or_pull_2.ToLower().Contains("update"))
                        {
                            Console.WriteLine("Only SELECT statement is allowed");
                            Environment.Exit(0);
                        }

                        // handle show progress
                        string[] possible_vals = { "0", "1" };
                        if (!possible_vals.Contains(push_or_pull_3))
                        {
                            Console.WriteLine("Show progress argument should be 1 or 0!");
                            Environment.Exit(0);
                        }
                    }
                    else
                    {
                        Console.WriteLine("Have no idea what " + push_or_pull_1.ToLower() + " is. program knows only push or pull as a first argument!");
                        Environment.Exit(0);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message.ToString());
                    Environment.Exit(0);
                }

                // 2) Check the directory: for pull and push
                try
                {
                    string just_csv_file_path = string.Empty;
                    string just_csv_name = string.Empty;
                    int index = csvFilePath.LastIndexOf("\\");
                    if (tableName != "null")
                    {
                        if (index > 0)
                        {
                            just_csv_file_path = csvFilePath.Substring(0, index); // or index + 1 to keep slash
                                                                                  //just_csv_name = csvFilePath.Substring(csvFilePath.LastIndexOf('\\') + 1);
                        }
                        if (!System.IO.Directory.Exists(just_csv_file_path) || !System.IO.File.Exists(csvFilePath))
                        {
                            Console.WriteLine("Folder " + just_csv_file_path + "\nor\nfile name " + just_csv_name + "\ndoesn't exist. You need to create it.");
                            Environment.Exit(0);
                        }
                    }
                    else
                    {
                        if (index > 0)
                        {
                            just_csv_file_path = csvFilePath.Substring(0, index); // or index + 1 to keep slash
                                                                                  //just_csv_name = csvFilePath.Substring(csvFilePath.LastIndexOf('\\') + 1);
                        }
                        if (!System.IO.Directory.Exists(just_csv_file_path))
                        {
                            Console.WriteLine("Folder " + just_csv_file_path + "\ndoesn't exist. You need to create it.");
                            Environment.Exit(0);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message.ToString());
                    Environment.Exit(0);
                }

                // 3) Check if table in sql db exists:
                try
                {
                    if (tableName != "null")
                    {
                        if (!Functions.IfSQLTableExists(tableName, connectionString))
                        {
                            // Console.WriteLine("Be sure " + tableName + " exists on SQL Server. Or you might forgot specify schema - e.g. dbo.TableName\n(DB schema has to be there because one table could exist under more than one different schemas!!)");
                            Console.WriteLine("Table " + tableName + " doesn't exit in database. Creating it...");
                            // Creating table function should be here:
                            Functions.CreateSQLTable(csvFilePath, 200001, tableName, connectionString);
                            Console.WriteLine("Table " + tableName + " has been created.");
                            newtable = true;
                            // Environment.Exit(0);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message.ToString());
                    Environment.Exit(0);
                }
                Console.WriteLine();
                #endregion

                //Console.WriteLine("------------------------------------------------");
                //Console.WriteLine("---------------Program is running---------------");
                //Console.WriteLine("------------------------------------------------");

                //Console.WriteLine();

                #region Program
                try
                {
                    if (push_or_pull_1.ToLower() == "push")
                    {
                        Stopwatch push = new Stopwatch();
                        push.Start();

                        // handle show progress
                        if (push_or_pull_3 == "1")
                        {
                            if (push_or_pull_2 == "1")
                            {
                                Console.WriteLine("Pushing data into " + tableName + " table with showing progress");
                                if (newtable)
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, true, connectionString, false);
                                }
                                else
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, true, connectionString, true);
                                }
                            }
                            else
                            {
                                Console.WriteLine("Pushing data into " + tableName + " table with showing progress");
                                Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, true, connectionString, false);
                            }
                        }
                        else
                        {
                            if (push_or_pull_2 == "1")
                            {
                                Console.WriteLine("Pushing data into " + tableName + " table without showing progress");
                                if (newtable)
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, false, connectionString, false);
                                }
                                else
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, false, connectionString, true);
                                }
                            }
                            else
                            {
                                Console.WriteLine("Pushing data into " + tableName + " table without showing progress");
                                Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, false, connectionString, false);
                            }
                        }
                        push.Stop();
                        Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMili seconds: {2}",
                            push.Elapsed.Minutes, push.Elapsed.Seconds, push.Elapsed.TotalMilliseconds);
                    }
                    else // pull
                    {
                        Stopwatch pull = new Stopwatch();
                        pull.Start();

                        // handle show progress parameter
                        if (push_or_pull_3 == "1")
                        {
                            Console.WriteLine("Downloading data from sql table into csv file with showing progress");
                            Functions.WriteFromDBToCSV(push_or_pull_2, csvFilePath, true, connectionString);
                        }
                        else
                        {
                            Console.WriteLine("Downloading data from sql table into csv file without showing progress");
                            Functions.WriteFromDBToCSV(push_or_pull_2, csvFilePath, false, connectionString);
                        }

                        pull.Stop();
                        Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMili seconds: {2}",
                            pull.Elapsed.Minutes, pull.Elapsed.Seconds, pull.Elapsed.TotalMilliseconds);
                    }

                    Console.WriteLine();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error message: " + ex.Message.ToString());
                    Environment.Exit(0);
                }
                #endregion
                // Console.ReadKey();
                // Thread.Sleep(5000);
            }
            else
            {
                Console.WriteLine("Invalid no of arguments should be 2, 4 or 6! Please, read documentation!");
            }
        }
    }
}
