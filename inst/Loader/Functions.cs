using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace csv_to_sql_loader
{
    public static class Functions
    {
        /// <summary>
        ///  Global variables for code below: input files, ftp/sql connection strings
        /// </summary>
        // public static string conStringName = ReturnConStringName();
        // public static string sqldbconnection = System.Configuration.ConfigurationManager.ConnectionStrings[conStringName].ConnectionString;

        // based on app approval output file columns

        public static void InsertDataIntoSQLServerUsingSQLBulkCopy_2(DataTable dtable, string sqlTableName, Int32 batch_size, string connString)
        {
            try
            {
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connString, System.Data.SqlClient.SqlBulkCopyOptions.TableLock))
                {
                    bulkCopy.DestinationTableName = sqlTableName;

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.BatchSize = batch_size;
                        // Set up the event handler to notify after 50 rows.
                        // bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(OnSqlRowsCopied);
                        // bulkCopy.NotifyAfter = 10000;
                        bulkCopy.WriteToServer(dtable);
                    }
                    catch (SqlException ex)
                    {
                        if (ex.Message.Contains("Received an invalid column length from the bcp client for colid"))
                        {
                            string pattern = @"\d+";
                            Match match = Regex.Match(ex.Message.ToString(), pattern);
                            var index = Convert.ToInt32(match.Value) - 1;

                            FieldInfo fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.NonPublic | BindingFlags.Instance);
                            var sortedColumns = fi.GetValue(bulkCopy);
                            var items = (Object[])sortedColumns.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sortedColumns);

                            FieldInfo itemdata = items[index].GetType().GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                            var metadata = itemdata.GetValue(items[index]);

                            var column = metadata.GetType().GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                            var length = metadata.GetType().GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);

                            Console.WriteLine("Error message: Column [" + column + "] contains data with a length greater than " + length);
                            Console.WriteLine();
                            Console.WriteLine("Table " + sqlTableName + " already exists in DB, just change data type - see the tip below.");
                            Console.WriteLine("Tip: try something like ALTER TABLE table_name ALTER COLUMN column_name datatype;");
                            // CleanUpTable(sqlTableName, connString);
                            Environment.Exit(1);
                        }
                        else
                        {
                            Console.WriteLine(ex.Message.ToString());
                            // CleanUpTable(sqlTableName, connString);
                            Environment.Exit(1);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message.ToString());
                        // CleanUpTable(sqlTableName, connString);
                        Environment.Exit(1);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message.ToString());
                Environment.Exit(1);
            }
        }

        public static void CleanUpTable(string sqlTableName, string connString)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(connString))
                {
                    con.Open();
                    string deleteRowsInTable = @"IF OBJECT_ID(" + "'" + sqlTableName + "','U')" +
                                                  " IS NOT NULL TRUNCATE TABLE " + sqlTableName + ";";
                    using (SqlCommand command = new SqlCommand(deleteRowsInTable, con))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }
                    con.Close();
                }
            }
            catch (SqlException sqlex)
            {
                Console.WriteLine("Truncate command cannot be used because of insufficient permissions: " + sqlex.Message.ToString());
                Console.WriteLine("DELETE FROM tabName is used instead.");
                using (SqlConnection con = new SqlConnection(connString))
                {
                    con.Open();
                    string deleteRowsInTable = @"IF OBJECT_ID(" + "'" + sqlTableName + "','U')" +
                                                  " IS NOT NULL DELETE FROM " + sqlTableName + ";";
                    using (SqlCommand command = new SqlCommand(deleteRowsInTable, con))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
        }

        public static void DropTable(string sqlTableName, string connString)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(connString))
                {
                    con.Open();
                    string deleteRowsInTable = @"IF OBJECT_ID(" + "'" + sqlTableName + "','U')" +
                                                  " IS NOT NULL DROP TABLE " + sqlTableName + ";";
                    using (SqlCommand command = new SqlCommand(deleteRowsInTable, con))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
        }

        public static void SQLQueryTask(string sqltask, string connString)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(connString))
                {
                    con.Open();
                    using (SqlCommand command = new SqlCommand(sqltask, con))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }
                    con.Close();
                    Console.WriteLine("Query has been completed!");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
        }

        public static string ReturnConStringName()
        {
            ConnectionStringSettingsCollection connections = ConfigurationManager.ConnectionStrings;
            string name = string.Empty;

            try
            {
                if (connections.Count != 0)
                {
                    foreach (ConnectionStringSettings connection in connections)
                    {
                        name = connection.Name;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }

            return name;
        }

        public static void ConvertCSVtoDataTable(string strFilePath, string tabName, Int32 flushed_batch_size,
                                                 bool showprogress, string connString, bool removeTab, char sep)
        {
            DataTable dt = new DataTable();
            Int64 rowsCount = 0;
            try
            {
                DataTable dataTypes = ExtractDataTypesFromSQLTable(tabName, connString);
                string str1 = string.Empty;
                int dt_rows_count = dataTypes.Rows.Count;

                // char sep = get_sep(strFilePath, dataTypes);
                // char sep = '\t';
                char[] seps = { '\t', ',', '.', ';', '|', '~', '^', ' '};
                foreach (char sprt in seps)
                {
                    if (sprt == sep)
                    {
                        Console.WriteLine("Tab is used as separator.");
                        break;
                    }
                    else if (sprt == sep)
                    {
                        Console.WriteLine("Comma is used as separator.");
                        break;
                    }
                    else if (sprt == sep)
                    {
                        Console.WriteLine("Dot is used as separator.");
                        break;
                    }   
                    else if (sprt == sep)
                    {
                        Console.WriteLine("Semi-colon is used as separator.");
                        break;
                    }
                    else if (sprt == sep)
                    {
                        Console.WriteLine("Pipe is used as separator.");
                        break;
                    }
                    else if (sprt == sep)
                    {
                        Console.WriteLine("Tilde is used as separator.");
                        break;
                    }
                    else if (sprt == sep)
                    {
                        Console.WriteLine("Caret is used as separator.");
                        break;
                    }
                    else if (sprt== sep)
                    {
                        Console.WriteLine("Space is used as separator.");
                        break;
                    }
                    else
                    {
                        Console.WriteLine("[" + sep + "] is used as separator.");
                        break;
                    }
                }
                
                using (StreamReader sr = new StreamReader(strFilePath))
                {
                    string[] headers = sr.ReadLine().Split(sep);

                    if (headers.Length != dt_rows_count)
                    {
                        Console.WriteLine("CSV file has different count of columns than table " + tabName + "!!!");
                        Console.WriteLine("Data Frame has " + headers.Length + " columns and table on SQL Server has " + dt_rows_count + " columns!");
                        Console.WriteLine("Tip: Try also check '" + sep + "' somewhere in the text in your DataFrame or DataTable you are trying to push to SQL Server,\nbecause tabulator is used as a separator!");
                        Environment.Exit(1);
                    }

                    // Compare header - CSV vs DataTable
                    for (int i = 0; i < dt_rows_count; i++)
                    {
                        DataRow drh = dataTypes.Rows[i];
                        //if (!headers[i].ToString().Contains(drh.ItemArray[0].ToString()))
                        if (headers[i].ToString().Replace("\"", "").ToLower() != drh.ItemArray[0].ToString().ToLower())
                        {
                            Console.WriteLine("You need to reorder columns in your csv according to columns in table " + tabName + "!!!");
                            Console.WriteLine("Column " + headers[i].ToString().Replace("\"", "") + " in your data.table or data.frame\ndoesn't correspond with column " + drh.ItemArray[0].ToString() + " defined in table " + tabName);
                            Environment.Exit(1);
                        }
                    }

                    if (removeTab)
                    {
                        Console.WriteLine("Cleaning table " + tabName);
                        CleanUpTable(tabName, connString);
                        Console.WriteLine("Table " + tabName + " has been cleaned");
                    }

                    for (int i = 0; i < dt_rows_count; i++)
                    {
                        DataRow dr = dataTypes.Rows[i];
                        // entire logic should goes here:
                        if (dr.ItemArray[1].ToString() == "float") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(double)); }
                        else if (dr.ItemArray[1].ToString() == "real") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(Single)); }
                        else if (dr.ItemArray[1].ToString() == "smallint") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(Int16)); }
                        else if (dr.ItemArray[1].ToString() == "int") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(Int32)); }
                        else if (dr.ItemArray[1].ToString() == "bigint") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(Int64)); }
                        else if (dr.ItemArray[1].ToString() == "bit") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(Boolean)); }
                        else if (dr.ItemArray[1].ToString() == "decimal" || dr.ItemArray[1].ToString() == "numeric") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(decimal)); }
                        else if (dr.ItemArray[1].ToString() == "uniqueidentifier") { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(Guid)); }
                        else { dt.Columns.Add(dr.ItemArray[0].ToString(), typeof(string)); }
                    }

                    Int64 batchsize = 0;

                    while (!sr.EndOfStream)
                    {
                        string[] rows = sr.ReadLine().Split(sep);

                        for (int i = 0; i < rows.Length; i++)
                        {
                            DataRow dtr = dataTypes.Rows[i];

                            if (rows[i] == "NA" || string.IsNullOrWhiteSpace(rows[i]))
                            {
                                rows[i] = null;
                            }
                            else
                            {
                                if (dtr.ItemArray[1].ToString() == "bigint") { rows[i] = Int64.Parse(rows[i], NumberStyles.Any).ToString(); }
                                else if (dtr.ItemArray[1].ToString() == "smallint") { rows[i] = Int16.Parse(rows[i], NumberStyles.Any).ToString(); }
                                else if (dtr.ItemArray[1].ToString() == "int") { rows[i] = Int32.Parse(rows[i], NumberStyles.Any).ToString(); }
                                else if (dtr.ItemArray[1].ToString() == "datetime") { rows[i] = DateTime.Parse(rows[i], null, DateTimeStyles.RoundtripKind).ToString(); }
                                else if (dtr.ItemArray[1].ToString() == "float") { rows[i] = double.Parse(rows[i], System.Globalization.NumberStyles.Any, CultureInfo.InvariantCulture).ToString(); }
                                else if (dtr.ItemArray[1].ToString() == "bit")
                                {
                                    Boolean.TryParse(StringExtensions.ToBoolean(rows[i]).ToString(), out bool parsedValue);
                                    rows[i] = parsedValue.ToString();
                                }
                                else if (dtr.ItemArray[1].ToString() == "real") { rows[i] = Single.Parse(rows[i], System.Globalization.NumberStyles.Any, CultureInfo.InvariantCulture).ToString(); }
                                else if (dtr.ItemArray[1].ToString() == "decimal" || dtr.ItemArray[1].ToString() == "numeric") { rows[i] = Decimal.Parse(rows[i], System.Globalization.NumberStyles.Any, CultureInfo.InvariantCulture).ToString(); }
                                else { rows[i] = rows[i].ToString().Replace("\"", ""); }
                            }
                        }

                        dt.Rows.Add(rows);
                        batchsize += 1;

                        if (batchsize == flushed_batch_size)
                        {
                            InsertDataIntoSQLServerUsingSQLBulkCopy_2(dt, tabName, flushed_batch_size, connString);
                            dt.Rows.Clear();
                            batchsize = 0;
                            if (showprogress) { Console.WriteLine("Flushing " + flushed_batch_size + " rows (" + (rowsCount + 1) + " records already imported)"); }
                        }
                        rowsCount += 1;
                        // rowCounter++;
                    }
                    InsertDataIntoSQLServerUsingSQLBulkCopy_2(dt, tabName, flushed_batch_size, connString);
                    dt.Rows.Clear();
                }
                Console.WriteLine(rowsCount + " records imported");
            }
            catch (FormatException fex)
            {
                Console.WriteLine(fex.Message.ToString());
                Console.WriteLine("Tip: there might be string between numeric data or the most likely escape character in string.\r\nCheck also scientific notation considered as string.");
                Environment.Exit(1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
            }
        }

        public static DataTable ExtractDataTypesFromSQLTable(string tabName, string connString)
        {
            DataTable table = new DataTable();
            try
            {
                using (SqlConnection con = new SqlConnection(connString))
                {
                    string sqlQuery = @"SELECT [Column Name],[Data type],[Max Length],[precision],[scale],[is_nullable],[Primary Key]
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
	                                        ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) r_number
                                        FROM    
                                            sys.columns c
                                        INNER JOIN 
                                            sys.types t ON c.user_type_id = t.user_type_id
                                        LEFT OUTER JOIN 
                                            sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                                        LEFT OUTER JOIN 
                                            sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                                        WHERE
                                            c.object_id = OBJECT_ID('" + tabName + "')) a ) b WHERE b.rn = 1 ORDER BY b.r_number";

                    using (SqlCommand cmd = new SqlCommand(sqlQuery, con))
                    {
                        SqlDataAdapter ds = new SqlDataAdapter(cmd);
                        ds.Fill(table);
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
            return table;
        }

        public static char get_sep(string strFilePath)
        {
            char sep;
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                IList<char> seps = new List<char>() { '\t', ',', '.', ';', '~', '|', '^' };
                sep = Convert.ToChar(AutoDetectCsvSeparator.Detect(sr, 250000, seps).ToString());
            }
            return sep;
        }

        public static bool IfSQLTableExists(string tabname, string connString)
        {
            bool exists = false;
            tabname = tabname.Replace("[", string.Empty).Replace("]", string.Empty);
            try
            {
                using (SqlConnection con = new SqlConnection(connString))
                {
                    con.Open();
                    using (SqlCommand command = new SqlCommand("SELECT CASE WHEN EXISTS((SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '" + tabname + "' OR TABLE_NAME = '" + tabname + "')) THEN 1 ELSE 0 END", con))
                    {
                        command.CommandTimeout = 0;
                        exists = (int)command.ExecuteScalar() == 1;
                    }
                    con.Close();
                }
            }
            catch (InvalidOperationException ioe)
            {
                Console.WriteLine("Invalid Operation Exception: " + ioe.Message.ToString() + "\nSomething is wrong with your connection string! You might check back slashes!");
                Environment.Exit(1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
            return (exists);
        }

        public static DataTable GetDataTableFromDataReader(IDataReader dataReader)
        {
            DataTable schemaTable = dataReader.GetSchemaTable();
            DataTable resultTable = new DataTable();

            foreach (DataRow dataRow in schemaTable.Rows)
            {
                DataColumn dataColumn = new DataColumn();
                dataColumn.ColumnName = dataRow["ColumnName"].ToString();
                dataColumn.DataType = Type.GetType(dataRow["DataType"].ToString());
                dataColumn.ReadOnly = (bool)dataRow["IsReadOnly"];
                dataColumn.AutoIncrement = (bool)dataRow["IsAutoIncrement"];
                dataColumn.Unique = (bool)dataRow["IsUnique"];

                resultTable.Columns.Add(dataColumn);
            }
            while (dataReader.Read())
            {
                DataRow dataRow = resultTable.NewRow();
                for (int i = 0; i < resultTable.Columns.Count; i++)
                {
                    dataRow[i] = dataReader[i];
                }
                resultTable.Rows.Add(dataRow);
            }
            return resultTable;
        }

        // Write data into csv:
        // https://social.msdn.microsoft.com/Forums/vstudio/en-US/8ef26d1e-b0a4-4cdc-ad0a-5dd7a7bcd333/large-csv-file-creator-in-c?forum=csharpgeneral
        public static void WriteFromDBToCSV(string sql_query, string csvpath, bool showprogress, string connString)
        {
            // DataTable dataTable = new DataTable();
            StringBuilder sb = new StringBuilder();
            DataTable dataTable = new DataTable();
            string sep = "~";

            try
            {
                using (SqlConnection con = new SqlConnection(connString))
                {
                    con.Open();
                    using (SqlCommand command = new SqlCommand(sql_query, con))
                    {
                        command.CommandTimeout = 0;

                        if (showprogress) { Console.WriteLine("Pushing data from SQL Server into DataTable"); }

                        using (IDataReader rdr = command.ExecuteReader())
                        {
                            dataTable = GetDataTableFromDataReader(rdr);
                        }
                        //IDataReader rdr = new SqlCommand(sql_query, con).ExecuteReader(CommandBehavior.CloseConnection);
                        //dataTable = GetDataTableFromDataReader(rdr);
                        //rdr = null;

                        //SqlDataAdapter da = new SqlDataAdapter(command);
                        //if (showprogress) { Console.WriteLine("Downloading data from sql server and pushing into DataTable object"); }
                        //da.Fill(dataTable);

                        if (showprogress) { Console.WriteLine("Pushing data from DataTable object into StringBuilder"); }

                        for (int i = 0; i < dataTable.Columns.Count; i++)
                        {
                            sb.Append(dataTable.Columns[i].ColumnName);
                            sb.Append(i == dataTable.Columns.Count - 1 ? "\n" : sep);
                        }

                        string day_s = string.Empty;
                        string month_s = string.Empty;
                        string value = string.Empty;
                        Int32 counter = 0;
                        Int32 c_ounter = 0;
                        // Writing data into csv file
                        foreach (DataRow row in dataTable.Rows)
                        {
                            for (int i = 0; i < dataTable.Columns.Count; i++)
                            {
                                if (row[i].GetType().Name == "DateTime")
                                {
                                    DateTime dt_val = (DateTime)row[i];
                                    if (dt_val.Month.ToString().Length == 1)
                                    {
                                        month_s = "0" + dt_val.Month.ToString();
                                    }
                                    else
                                    {
                                        month_s = dt_val.Month.ToString();
                                    }
                                    if (dt_val.Day.ToString().Length == 1)
                                    {
                                        day_s = "0" + dt_val.Day.ToString();
                                    }
                                    else
                                    {
                                        day_s = dt_val.Day.ToString();
                                    }
                                    if (dt_val.Hour == 0 & dt_val.Minute == 0 & dt_val.Second == 0 & dt_val.Millisecond == 0)
                                    {
                                        value = dt_val.Year.ToString() + "-" + month_s + "-" + day_s;
                                    }
                                    else
                                    {
                                        value = dt_val.Year.ToString() + "-" + month_s + "-" + day_s + " " + dt_val.TimeOfDay.ToString();
                                    }
                                    sb.Append(value);
                                    sb.Append(i == dataTable.Columns.Count - 1 ? "\n" : sep);
                                }
                                else if (row[i].GetType().Name == "Decimal" |
                                        row[i].GetType().Name == "Numeric" |
                                        row[i].GetType().Name == "Float" |
                                        row[i].GetType().Name == "Double" |
                                        row[i].GetType().Name == "Single")
                                {
                                    Double val;
                                    if (double.TryParse(row[i].ToString(), out val))
                                    {
                                        sb.Append(val.ToString(CultureInfo.InvariantCulture));
                                        sb.Append(i == dataTable.Columns.Count - 1 ? "\n" : sep);
                                    }
                                }
                                else
                                {
                                    sb.Append(row[i].ToString());
                                    sb.Append(i == dataTable.Columns.Count - 1 ? "\n" : sep);
                                }
                            }
                            counter++;
                            c_ounter++;
                            if (c_ounter == 100000 & showprogress)
                            {
                                Console.WriteLine(counter + " rows inserted from StringBuilder --> csv.");
                                File.AppendAllText(csvpath, sb.ToString());
                                sb.Clear();
                                c_ounter = 0;
                            }
                        }
                        if (sb.Length != 0)
                        {
                            File.AppendAllText(csvpath, sb.ToString());
                        }
                        Console.WriteLine(counter + " records written into DataFrame/DataTable.");
                        // if (showprogress) { Console.WriteLine("Writing from StringBuilder into csv file."); }
                        // File.WriteAllText(csvpath, sb.ToString());
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
        }

        // Write sql data directly to flat file
        public static void WriteToFileFromDB(string sql_query, string csvpath, bool showprogress, string connString)
        {
            try
            {
                // define separator
                string sep = "~";
                // define counter for writing into flat file
                Int32 cntr = 0;
                Int32 cntr_overall = 0;
                //create connection
                SqlCommand comm = new SqlCommand
                {
                    Connection = new SqlConnection(connString)
                };
                String sql = sql_query;

                comm.CommandTimeout = 0;
                comm.CommandText = sql;
                comm.Connection.Open();

                SqlDataReader sqlReader = comm.ExecuteReader();

                // Open the file for write operations.  If exists, it will overwrite due to the "false" parameter
                using (StreamWriter file = new StreamWriter(csvpath, false))
                {
                    object[] output = new object[sqlReader.FieldCount];

                    for (int i = 0; i < sqlReader.FieldCount; i++)
                        output[i] = sqlReader.GetName(i);

                    file.WriteLine(string.Join(sep, output));

                    while (sqlReader.Read())
                    {
                        sqlReader.GetValues(output);

                        string day_s = string.Empty;
                        string month_s = string.Empty;
                        Int32 counter = 0;

                        foreach (var val in output)
                        {
                            if (val.GetType().Name == "DateTime")
                            {
                                DateTime dt_val = (DateTime)val;
                                if (dt_val.Month.ToString().Length == 1)
                                {
                                    month_s = "0" + dt_val.Month.ToString();
                                }
                                else
                                {
                                    month_s = dt_val.Month.ToString();
                                }
                                if (dt_val.Day.ToString().Length == 1)
                                {
                                    day_s = "0" + dt_val.Day.ToString();
                                }
                                else
                                {
                                    day_s = dt_val.Day.ToString();
                                }
                                if (dt_val.Hour == 0 & dt_val.Minute == 0 & dt_val.Second == 0 & dt_val.Millisecond == 0)
                                {
                                    output[counter] = dt_val.Year.ToString() + "-" + month_s + "-" + day_s;
                                }
                                else
                                {
                                    output[counter] = dt_val.Year.ToString() + "-" + month_s + "-" + day_s + " " + dt_val.TimeOfDay.ToString();
                                }
                            }
                            else if (val.GetType().Name == "Decimal" |
                                    val.GetType().Name == "Numeric" |
                                    val.GetType().Name == "Float" |
                                    val.GetType().Name == "Double" |
                                    val.GetType().Name == "Single")
                            {
                                Double numval;
                                if (double.TryParse(val.ToString(), out numval))
                                {
                                    output[counter] = numval.ToString(CultureInfo.InvariantCulture);
                                }
                            }
                            else
                            {
                                output[counter] = val.ToString();
                            }
                            counter += 1;
                        }
                        if (cntr == 100000 && showprogress)
                        {
                            Console.WriteLine("Flushed 100000 records into flat file, " + cntr_overall + " records are already there.");
                            cntr = 0;
                        }
                        file.WriteLine(string.Join(sep, output));
                        cntr_overall += 1;
                        cntr += 1;
                    }
                }
                comm.Connection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
        }

        public static void CreateSQLTable(string pathtocsv, Int32 rowstoestimatedatatype, string tablename, string connstring, char sep)
        {
            using (StreamReader sr = new StreamReader(pathtocsv))
            {
                // IList<char> seps = new List<char>() { '\t', ',', '.', ';' };
                //separator = Convert.ToChar(AutoDetectCsvSeparator.Detect(sr, 100000, seps).ToString());
            }
            string[,] sqldts = DataTypeIdentifier.SQLDataTypes(pathtocsv, rowstoestimatedatatype, sep);

            string createTable_string = string.Empty;

            for (int i = 0; i < sqldts.GetLength(1); i++)
            {
                if (i == sqldts.GetLength(1) - 1)
                {
                    createTable_string = createTable_string + "[" + sqldts[1, i] + "]" + " " + sqldts[0, i];
                }
                else
                {
                    createTable_string = createTable_string + "[" + sqldts[1, i] + "]" + " " + sqldts[0, i] + ", ";
                }
            }
            try
            {
                using (SqlConnection con = new SqlConnection(connstring))
                {
                    con.Open();
                    string createTable = @"CREATE TABLE " + tablename + " (" + createTable_string + ");";
                    using (SqlCommand command = new SqlCommand(createTable, con))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(1);
            }
        }
        public static Tuple<bool, string> IsServerConnected(string connectionString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    var tpl = new Tuple<bool, string>(true, string.Empty);
                    return tpl;
                }
                catch (SqlException ex)
                {
                    string msg = "SqlException message: " + ex.Message.ToString();
                    var tpl = new Tuple<bool, string>(false, msg);
                    return tpl;
                }
            }
        }
    }
}