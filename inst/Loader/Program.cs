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
            //Functions.CreateSQLTable("c:\\test.csv", 200001, "[dbo].[mv.New.Table]",
            //    "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=DatabaseName;Integrated Security=True;");

            //Functions.WriteFromDBToCSV("select top 200000 * from [dbo].[DataTable]", "c:\\data.csv", true,
            //    "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=DatabaseName;Integrated Security=True;");

            //string[] args = { "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=DatabaseName;Integrated Security=True;", "c:\\data.csv",
            //    "[dbo].[test.Table]", "push", "0", "0", "|" };

            //string[] args = { "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=Data;Integrated Security=True;", "dbinfo",
            //    "c:\\temp.csv", null };

            // string[] args = { "select * from some tab" };
            // string conn_string = "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=Data;Integrated Security=True;";
            //Functions.WriteFromDBToCSV(@"select top 5 * from [dbo].[DataTable]",
            //    "c:\\data3.csv", true, conn_string);

            // Test connection:
            Tuple<bool, string> isConnected = Functions.IsServerConnected(args[0]);
            if (!isConnected.Item1)
            {
                Console.WriteLine(isConnected.Item2);
                Environment.Exit(1);
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
                    if (!Functions.IfSQLTableExists(args[3], args[0]))
                    {
                        Console.WriteLine("Table " + args[3] + " doesn't exist.");
                        Environment.Exit(1);
                    }
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
                    string dbinfo_sql = @"SELECT
	                                        t2.TABLE_NAME TableName
	                                        ,t2.TABLE_SCHEMA SchemaName
	                                        ,t1.RowCounts
	                                        ,t2.cols_count ColCounts
	                                        ,t2.table_type TableType
	                                        ,t2.table_catalog TableCatalog
	                                        ,t2.last_modified LastModified
	                                        ,t1.TotalSpaceKB
	                                        ,t1.TotalSpaceMB
	                                        ,cast(t1.TotalSpaceMB/1024.00 as numeric(36,2)) TotalSpaceGB
	                                        ,t1.UsedSpaceKB
	                                        ,t1.UsedSpaceMB
	                                        ,t1.UnusedSpaceKB
	                                        ,t1.UnusedSpaceMB
                                        from
	                                        (
	                                        SELECT TableName,SchemaName,RowCounts,TotalSpaceKB,TotalSpaceMB,UsedSpaceKB,UsedSpaceMB,UnusedSpaceKB,UnusedSpaceMB from
	                                        (
	                                        SELECT
		                                           tab.*
		                                           ,ROW_NUMBER() OVER(PARTITION BY schemaname, tablename ORDER BY TotalSpaceKB desc) rn
	                                        FROM
	                                        (
	                                        SELECT
		                                        t.NAME AS TableName,
		                                        s.Name AS SchemaName,
		                                        p.rows AS RowCounts,
		                                        SUM(a.total_pages) * 8 AS TotalSpaceKB, 
		                                        CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB,
		                                        SUM(a.used_pages) * 8 AS UsedSpaceKB, 
		                                        CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS UsedSpaceMB, 
		                                        (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB,
		                                        CAST(ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS NUMERIC(36, 2)) AS UnusedSpaceMB
	                                        FROM 
		                                        sys.tables t
	                                        INNER JOIN      
		                                        sys.indexes i ON t.OBJECT_ID = i.object_id
	                                        INNER JOIN 
		                                        sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
	                                        INNER JOIN 
		                                        sys.allocation_units a ON p.partition_id = a.container_id
	                                        LEFT OUTER JOIN 
		                                        sys.schemas s ON t.schema_id = s.schema_id
	                                        GROUP BY 
		                                        t.Name, s.Name, p.Rows
	                                        ) tab
	                                        ) result
	                                        where rn = 1
	                                        ) t1
	                                        right join
	                                        (
	                                        -- part 2
	                                        SELECT * from
	                                        (
	                                        SELECT distinct entireView.*
		                                        ,case
			                                        when sys_stats.[last_user_update] is null
				                                        then st.[modify_date]
			                                        else sys_stats.[last_user_update]
		                                        end as [LAST_MODIFIED]
		                                        ,row_number() over(partition by entireView.[TABLE_SCHEMA],entireView.[TABLE_NAME] order by entireView.[ROWS_COUNT] desc) rn
	                                        from
		                                        (
		                                        SELECT origTab.[TABLE_SCHEMA],origTab.[TABLE_NAME],origTab.[TABLE_TYPE],origTab.[TABLE_CATALOG],origTab.[ROWS_COUNT],colTab.[COLS_COUNT]
			                                        from
			                                        (
			                                        SELECT isc.[TABLE_NAME], count(distinct [COLUMN_NAME]) [COLS_COUNT],
			                                        'BASE TABLE' [TABLE_TYPE]
			                                        from INFORMATION_SCHEMA.COLUMNS isc
			                                        group by isc.[TABLE_NAME]
			                                        ) colTab
			                                        right join
			                                        (
			                                        SELECT [TABLE_SCHEMA], tbl.name as [TABLE_NAME],[TABLE_TYPE],[TABLE_CATALOG],[rows] as [ROWS_COUNT] from
			                                        (
			                                        SELECT name,id,ist.TABLE_TYPE,ist.TABLE_CATALOG,ist.TABLE_SCHEMA from
			                                        (
			                                        SELECT name, id, case xtype
				                                        when 'U' then 'BASE TABLE'
				                                        when 'V' then 'VIEW'
				                                        end as [TABLE_TYPE]
			                                        FROM sysobjects
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
			                                        ) tab
		                                        where rn = 1
		                                        ) t2
		                                        on t1.tablename=t2.TABLE_NAME and t1.schemaname=t2.TABLE_SCHEMA
		                                        order by t2.TABLE_NAME";
                    Functions.WriteFromDBToCSV(dbinfo_sql, args[2], false, args[0]);
                    Console.WriteLine("Basic info about database has been created.");
                    Environment.Exit(0);
                }
                // Another SQL query task
                else
                {
                    Console.WriteLine("Not sure what you are trying to achieve... Please, read the documentation!");
                    Environment.Exit(1);
                }

                sqltask.Stop();
                Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMilliseconds: {2}",
                    sqltask.Elapsed.Minutes, sqltask.Elapsed.Seconds, sqltask.Elapsed.TotalMilliseconds);
                Environment.Exit(0);
            }
            else if (args.Length == 6 || args.Length == 7)
            {
                string connectionString = args[0]; // "Data Source=LAPTOP-USERNAME\\SQLEXPRESS;Initial Catalog=Data;Integrated Security=True;"
                string csvFilePath = args[1]; // "C:\\pathToFile\\data.csv"
                string tableName = args[2]; // "dbo.DataTable"
                                            // push: push, delete tab, show progress
                                            // pull: pull, sql query, show progress
                string push_or_pull_1 = args[3]; // "push"
                string push_or_pull_2 = args[4]; // "1"
                string push_or_pull_3 = args[5]; // "1"

                char separator = '\0'; // for push method

                if (args.Length == 7)
                {
                    separator = char.Parse(args[6]); // this is for separator
                }
                
                #region Input validation
                // Validate inputs:
                // 1) Check array
                // push: push, delete tab, show progress
                // pull: pull, sql query, show progress


                // 2) Check the directory: for pull and push
                Functions.CheckDirectory(csvFilePath, tableName);

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
                            Functions.CreateSQLTable(csvFilePath, 250001, tableName, connectionString, separator);
                            Console.WriteLine("Table " + tableName + " has been created.");
                            newtable = true;
                            // Environment.Exit(1);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message.ToString());
                    Environment.Exit(1);
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
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, true, connectionString, false, separator);
                                }
                                else
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, true, connectionString, true, separator);
                                }
                            }
                            else
                            {
                                Console.WriteLine("Pushing data into " + tableName + " table with showing progress");
                                Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, true, connectionString, false, separator);
                            }
                            push.Stop();
                            Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMilliseconds: {2}",
                                push.Elapsed.Minutes, push.Elapsed.Seconds, push.Elapsed.TotalMilliseconds);
                        }
                        else
                        {
                            if (push_or_pull_2 == "1")
                            {
                                //Console.WriteLine("Pushing data into " + tableName + " table without showing progress");
                                if (newtable)
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, false, connectionString, false, separator);
                                }
                                else
                                {
                                    Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, false, connectionString, true, separator);
                                }
                            }
                            else
                            {
                                //Console.WriteLine("Pushing data into " + tableName + " table without showing progress");
                                Functions.ConvertCSVtoDataTable(csvFilePath, tableName, 100000, false, connectionString, false, separator);
                            }
                        }
                    }
                    else if (push_or_pull_1.ToLower() == "pull")
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
                        Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMilliseconds: {2}",
                            pull.Elapsed.Minutes, pull.Elapsed.Seconds, pull.Elapsed.TotalMilliseconds);
                    }
                    else if (push_or_pull_1.ToLower() == "dpull")
                    {
                        Stopwatch pull = new Stopwatch();
                        pull.Start();

                        // handle show progress parameter
                        if (push_or_pull_3 == "1")
                        {
                            Console.WriteLine("Downloading data from sql table into csv file with showing progress");
                            Functions.WriteToFileFromDB(push_or_pull_2, csvFilePath, true, connectionString);
                        }
                        else
                        {
                            Console.WriteLine("Downloading data from sql table into csv file without showing progress");
                            Functions.WriteToFileFromDB(push_or_pull_2, csvFilePath, false, connectionString);
                        }

                        pull.Stop();
                        Console.WriteLine("This operation took\n" + "Minutes: {0}\nSeconds: {1}\nMilliseconds: {2}",
                            pull.Elapsed.Minutes, pull.Elapsed.Seconds, pull.Elapsed.TotalMilliseconds);
                    }
                    Console.WriteLine();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error message: " + ex.Message.ToString());
                    Environment.Exit(1);
                }
                #endregion
                // Console.ReadKey();
                // Thread.Sleep(5000);
            }
            else
            {
                Console.WriteLine("Invalid no of arguments should be 2, 4, 6 or 7! Please, read documentation!");
                Environment.Exit(1);
            }
            Environment.Exit(0);
        }
    }
}