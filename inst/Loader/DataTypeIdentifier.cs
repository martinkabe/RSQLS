using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace csv_to_sql_loader
{
    public static class DataTypeIdentifier
    {
        // check double type
        public static bool IsDoubleType(DataTable dt, int colNumber)
        {
            bool result = false;
            double doubleValue;
            DataColumn dc = dt.Columns[colNumber];
            foreach (DataRow row in dt.Select())
            {
                string colval = row[dc].ToString();
                if (string.IsNullOrWhiteSpace(colval)) { continue; }
                result = double.TryParse(colval, System.Globalization.NumberStyles.Any, CultureInfo.InvariantCulture, out doubleValue);
                if (!result) { break; }
            }
            return result;
        }

        // check int type
        public static bool IsIntType(DataTable dt, int colNumber)
        {
            bool result = false;
            Int32 intValue;
            DataColumn dc = dt.Columns[colNumber];
            foreach (DataRow row in dt.Select())
            {
                string colval = row[dc].ToString();
                if (string.IsNullOrWhiteSpace(colval)) { continue; }
                result = Int32.TryParse(colval, NumberStyles.Any, CultureInfo.InvariantCulture, out intValue);
                if (!result) { break; }
            }
            return result;
        }

        // check bool type
        public static bool IsBoolType(DataTable dt, int colNumber)
        {
            bool result = false;
            bool boolValue;
            DataColumn dc = dt.Columns[colNumber];
            foreach (DataRow row in dt.Select())
            {
                string colval = row[dc].ToString();
                if (string.IsNullOrWhiteSpace(colval)) { continue; }
                result = bool.TryParse(colval, out boolValue);
                if (!result) { break; }
            }
            return result;
        }

        // check datetime type
        public static bool IsDateTimeType(DataTable dt, int colNumber)
        {
            bool result = false;
            DateTime dateTimeValue;
            DataColumn dc = dt.Columns[colNumber];
            foreach (DataRow row in dt.Select())
            {
                string colval = row[dc].ToString();
                if (string.IsNullOrWhiteSpace(colval)) { continue; }
                result = DateTime.TryParse(colval, out dateTimeValue);
                if (result == false || colval.Contains(":") == false) {result = false; break; }
            }
            return result;
        }

        public static bool IsDateType(DataTable dt, int colNumber)
        {
            bool result = false;
            DateTime dateTimeValue;
            DataColumn dc = dt.Columns[colNumber];
            foreach (DataRow row in dt.Select())
            {
                string colval = row[dc].ToString();
                if (string.IsNullOrWhiteSpace(colval)) { continue; }
                result = DateTime.TryParse(colval, out dateTimeValue);
                if (result == false || colval.Contains(":") == true) {result = false; break; }
            }
            return result;
        }

        public static Int32 maxStringLength(DataTable dt, int colNumber)
        {
            DataColumn dc = dt.Columns[colNumber];
            Int32 maxLength = 0;
            foreach (DataRow row in dt.Select())
            {
                string colval = row[dc].ToString();
                if (string.IsNullOrWhiteSpace(colval)) { continue; }
                if (colval.Length > maxLength)
                {
                    maxLength = colval.Length;
                }
            }
            maxLength = Convert.ToInt32(maxLength * 1.5);
            return maxLength;
        }

        // Return sql data types:
        public static string[,] SQLDataTypes(string pathtocsv, Int32 norowsreview, char sep)
        {
            DataTable dt = CSVReader.csvToDataTable(pathtocsv, norowsreview, sep);
            string[,] sqldatatype_array = new string[2, dt.Columns.Count];

            try
            {
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    //// Basic logic should goes here:
                    // Check bool type
                    if (DataTypeIdentifier.IsBoolType(dt, i))
                    {
                        sqldatatype_array[0, i] = "bool";
                        sqldatatype_array[1, i] = dt.Columns[i].ToString();
                        continue;
                    }
                    // Check int type
                    else if (DataTypeIdentifier.IsIntType(dt, i))
                    {
                        sqldatatype_array[0, i] = "int";
                        sqldatatype_array[1, i] = dt.Columns[i].ToString();
                        continue;
                    }
                    // Check double
                    else if (DataTypeIdentifier.IsDoubleType(dt, i))
                    {
                        sqldatatype_array[0, i] = "float";
                        sqldatatype_array[1, i] = dt.Columns[i].ToString();
                        continue;
                    }
                    // check date
                    else if (DataTypeIdentifier.IsDateType(dt, i))
                    {
                        sqldatatype_array[0, i] = "date";
                        sqldatatype_array[1, i] = dt.Columns[i].ToString();
                        continue;
                    }
                    // check datetime
                    else if (DataTypeIdentifier.IsDateTimeType(dt, i))
                    {
                        sqldatatype_array[0, i] = "datetime";
                        sqldatatype_array[1, i] = dt.Columns[i].ToString();
                        continue;
                    }
                    // else string
                    else
                    {
                        Int32 varcharLength = maxStringLength(dt, i);
                        sqldatatype_array[0, i] = "varchar(" + varcharLength + ")";
                        sqldatatype_array[1, i] = dt.Columns[i].ToString();
                        continue;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());
                Environment.Exit(0);
            }
            return sqldatatype_array;
        }
    }
}
