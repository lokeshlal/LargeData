using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LargeData.Helpers
{
    internal class FileHelper
    {
        public static string CreateFiles(string guid, DataSet dataset, string temporaryLocation)
        {
            string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
            string rootDirectory = Path.Combine(temporaryLocation, taskDirectoryName);
            Directory.CreateDirectory(rootDirectory);

            int order = 1;

            foreach (DataTable dt in dataset.Tables)
            {
                int fileCount = 1;
                string tableName = !string.IsNullOrEmpty(dt.TableName) ? dt.TableName : string.Format("Table{0}", order);


                var reader = dt.CreateDataReader();
                DataTable tableSchema = reader.GetSchemaTable();

                bool containsIdentityColumn = false;
                Dictionary<string, FieldValue> dataTypeRow = new Dictionary<string, FieldValue>();
                containsIdentityColumn = GetFileHeaderRow(tableSchema, containsIdentityColumn, dataTypeRow);

                int recordsProcessed = 0;
                Dictionary<string, FieldValue> row;
                List<Dictionary<string, FieldValue>> rows = new List<Dictionary<string, FieldValue>>();

                // using data reader, to avoid out of memory exception and fast and seemless data transfer
                while (reader.Read())
                {
                    row = new Dictionary<string, FieldValue>();
                    Dictionary<string, object> fieldValuesMapping = new Dictionary<string, object>();

                    foreach (DataRow dr in tableSchema.Rows)
                    {
                        string columnName = dr["ColumnName"] as string;

                        //string dataTypeName = dr["DataTypeName"] as string;
                        string dataTypeName = "System.String";
                        if (dr["DataType"] != null)
                        {
                            dataTypeName = ((Type)(dr["DataType"])).FullName as string;
                        }
                        if (dataTypeName == "timestamp") continue;

                        Type dataType = typeof(int);

                        FieldValue value = new FieldValue();

                        switch (dataTypeName)
                        {
                            case "System.Int32": // change type
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.IntValue = null;
                                }
                                else
                                {
                                    value.IntValue = Convert.ToInt32(reader[columnName]);
                                }
                                break;
                            case "System.Int64": // change type
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.LongValue = null;
                                }
                                else
                                {
                                    value.LongValue = Convert.ToInt64(reader[columnName]);
                                }
                                break;
                            case "System.DateTimeOffset":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.DateTimeOffsetValue = null;
                                }
                                else
                                {
                                    value.DateTimeOffsetValue = (DateTimeOffset)(reader[columnName]);
                                }
                                break;
                            case "System.DateTime":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.DateTimeValue = null;
                                }
                                else
                                {
                                    value.DateTimeValue = (DateTime)(reader[columnName]);
                                }
                                break;
                            case "System.String":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.StringValue = null;
                                }
                                else
                                {
                                    value.StringValue = Convert.ToString(reader[columnName]);
                                }
                                break;
                            case "System.Boolean":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.BoolValue = null;
                                }
                                else
                                {
                                    value.BoolValue = Convert.ToBoolean(reader[columnName]);
                                }
                                break;
                            case "System.Byte[]":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.ByteValue = null;
                                }
                                else
                                {
                                    value.ByteValue = (byte[])(reader[columnName]);
                                }
                                break;
                            case "System.Guid":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.GuidValue = null;
                                }
                                else
                                {
                                    value.GuidValue = (Guid)(reader[columnName]);
                                }
                                break;
                            case "System.Decimal":
                            case "System.Single":
                            case "System.Double":
                                if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                                {
                                    value.DecimalValue = null;
                                }
                                else
                                {
                                    value.DecimalValue = Convert.ToDecimal(reader[columnName]);
                                }
                                break;
                        }
                        row.Add(columnName, value);
                    }

                    rows.Add(row);

                    recordsProcessed++;
                    if (recordsProcessed >= ServerSettings.MaxRecordsInAFile)
                    {
                        recordsProcessed = FileHelper.CreateFile(rootDirectory, order, ref fileCount, tableName, containsIdentityColumn, dataTypeRow, rows);
                    }
                }

                if (recordsProcessed > 0)
                {
                    recordsProcessed = FileHelper.CreateFile(rootDirectory, order, ref fileCount, tableName, containsIdentityColumn, dataTypeRow, rows);
                }

                reader.Close();
                order++;
            }

            return rootDirectory;
        }

        private static bool GetFileHeaderRow(DataTable tableSchema, bool containsIdentityColumn, Dictionary<string, FieldValue> dataTypeRow)
        {
            foreach (DataRow dr in tableSchema.Rows)
            {
                //if(tableSchema.Columns.Contains("IsIdentity"))
                bool isIdentity = Convert.ToBoolean(dr["IsKey"]); //IsIdentity
                if (isIdentity) containsIdentityColumn = true;

                //string dataTypeName = dr["DataTypeName"] as string; // DataTypeName is present when created directly from reader
                string dataTypeName = "System.String";
                if (dr["DataType"] != null)
                {
                    dataTypeName = ((Type)(dr["DataType"])).FullName as string;
                }
                //Type dataType = typeof(int);

                string columnName = dr["ColumnName"] as string;

                dataTypeRow.Add(columnName, new FieldValue() { StringValue = dataTypeName });
            }

            return containsIdentityColumn;
        }

        public static string CreateFilesUsingReader(string guid, IDataReader reader, string temporaryLocation)
        {
            string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
            string rootDirectory = Path.Combine(temporaryLocation, taskDirectoryName);
            Directory.CreateDirectory(rootDirectory);

            int order = 1;
            int fileCount = 1;

            // first table
            string tableName = string.Format("Table{0}", order);
            bool firstRun = true;
            int recordsProcessed = 0;
            Dictionary<string, FieldValue> row = new Dictionary<string, FieldValue>();
            List<Dictionary<string, FieldValue>> rows = new List<Dictionary<string, FieldValue>>();
            DataTable tableSchema = new DataTable();
            bool containsIdentityColumn = false;
            Dictionary<string, FieldValue> dataTypeRow = new Dictionary<string, FieldValue>();
            ConvertReaderToFile(reader, rootDirectory, ref order, ref fileCount, tableName, ref firstRun, ref recordsProcessed, ref row, rows, ref tableSchema, ref containsIdentityColumn, dataTypeRow);

            // 2nd onwards table
            while (reader.NextResult())
            {
                // reset variables for next result set
                tableName = string.Format("Table{0}", order);
                firstRun = true;
                recordsProcessed = 0;
                rows = new List<Dictionary<string, FieldValue>>();
                tableSchema = new DataTable();
                containsIdentityColumn = false;
                dataTypeRow = new Dictionary<string, FieldValue>();
                ConvertReaderToFile(reader, rootDirectory, ref order, ref fileCount, tableName, ref firstRun, ref recordsProcessed, ref row, rows, ref tableSchema, ref containsIdentityColumn, dataTypeRow);
            }
            reader.Close();
            return rootDirectory;
        }

        /// <summary>
        ///  converts data reader result set to file
        /// </summary>
        /// <param name="reader">data reader</param>
        /// <param name="rootDirectory">root directory</param>
        /// <param name="order">order of table</param>
        /// <param name="fileCount">file count number - incremental</param>
        /// <param name="tableName">table name</param>
        /// <param name="firstRun">is first run parameter, not required here, will refactor later</param>
        /// <param name="recordsProcessed">not required, will refactor later</param>
        /// <param name="row">row, not required here, refactoring required</param>
        /// <param name="rows">rows, , not required here, refactoring required</param>
        /// <param name="tableSchema">, not required here, refactoring required</param>
        /// <param name="containsIdentityColumn">, not required here, refactoring required</param>
        /// <param name="dataTypeRow">, not required here, refactoring required</param>
        private static void ConvertReaderToFile(IDataReader reader, string rootDirectory, ref int order, ref int fileCount, string tableName, ref bool firstRun, ref int recordsProcessed, ref Dictionary<string, FieldValue> row, List<Dictionary<string, FieldValue>> rows, ref DataTable tableSchema, ref bool containsIdentityColumn, Dictionary<string, FieldValue> dataTypeRow)
        {
            while (reader.Read())
            {
                if (firstRun)
                {
                    tableSchema = reader.GetSchemaTable();
                    containsIdentityColumn = GetFileHeaderRow(tableSchema, containsIdentityColumn, dataTypeRow);
                    firstRun = false; // set it to false for current result set
                }
                row = new Dictionary<string, FieldValue>();
                Dictionary<string, object> fieldValuesMapping = new Dictionary<string, object>();

                foreach (DataRow dr in tableSchema.Rows)
                {
                    string columnName = dr["ColumnName"] as string;

                    //string dataTypeName = dr["DataTypeName"] as string;
                    string dataTypeName = "System.String";
                    if (dr["DataType"] != null)
                    {
                        dataTypeName = ((Type)(dr["DataType"])).FullName as string;
                    }
                    if (dataTypeName == "timestamp") continue;

                    Type dataType = typeof(int);

                    FieldValue value = new FieldValue();

                    switch (dataTypeName)
                    {
                        case "System.Int32": // change type
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.IntValue = null;
                            }
                            else
                            {
                                value.IntValue = Convert.ToInt32(reader[columnName]);
                            }
                            break;
                        case "System.Int64": // change type
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.LongValue = null;
                            }
                            else
                            {
                                value.LongValue = Convert.ToInt64(reader[columnName]);
                            }
                            break;
                        case "System.DateTimeOffset":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.DateTimeOffsetValue = null;
                            }
                            else
                            {
                                value.DateTimeOffsetValue = (DateTimeOffset)(reader[columnName]);
                            }
                            break;
                        case "System.DateTime":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.DateTimeValue = null;
                            }
                            else
                            {
                                value.DateTimeValue = (DateTime)(reader[columnName]);
                            }
                            break;
                        case "System.String":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.StringValue = null;
                            }
                            else
                            {
                                value.StringValue = Convert.ToString(reader[columnName]);
                            }
                            break;
                        case "System.Boolean":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.BoolValue = null;
                            }
                            else
                            {
                                value.BoolValue = Convert.ToBoolean(reader[columnName]);
                            }
                            break;
                        case "System.Byte[]":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.ByteValue = null;
                            }
                            else
                            {
                                value.ByteValue = (byte[])(reader[columnName]);
                            }
                            break;
                        case "System.Guid":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.GuidValue = null;
                            }
                            else
                            {
                                value.GuidValue = (Guid)(reader[columnName]);
                            }
                            break;
                        case "System.Decimal":
                        case "System.Single":
                        case "System.Double":
                            if (reader[columnName] == null || reader[columnName] == DBNull.Value)
                            {
                                value.DecimalValue = null;
                            }
                            else
                            {
                                value.DecimalValue = Convert.ToDecimal(reader[columnName]);
                            }
                            break;
                    }
                    row.Add(columnName, value);
                }
                rows.Add(row);
                recordsProcessed++;
                if (recordsProcessed >= ServerSettings.MaxRecordsInAFile)
                {
                    recordsProcessed = FileHelper.CreateFile(rootDirectory, order, ref fileCount, tableName, containsIdentityColumn, dataTypeRow, rows);
                }
            }
            if (recordsProcessed > 0)
            {
                recordsProcessed = FileHelper.CreateFile(rootDirectory, order, ref fileCount, tableName, containsIdentityColumn, dataTypeRow, rows);
            }
            order++;
        }

        public static int CreateFile(string rootDirectory, int order, ref int fileCount, string tableName, bool containsIdentityColumn, Dictionary<string, FieldValue> dataTypeRow, List<Dictionary<string, FieldValue>> rows)
        {
            int recordsProcessed;
            string tempFileName = Path.Combine(rootDirectory, CreateFile(order, tableName, fileCount, containsIdentityColumn));
            fileCount++;
            rows.Insert(0, dataTypeRow);
            JsonSerializeToString(rows, tempFileName);
            recordsProcessed = 0;
            rows.Clear();
            return recordsProcessed;
        }

        public static string CreateFile(int order, string tableName, int fileCount, bool containsIdentityColumn)
        {
            string fileName = string.Format("{0}-{1}-{2}-{3}.json",
                order,
                tableName,
                fileCount,
                containsIdentityColumn);
            return fileName;
        }

        public static void JsonSerializeToString<TData>(TData obj, string fileName)
        {
            // serialize JSON directly to a file
            using (StreamWriter file = File.CreateText(fileName))
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Culture = System.Globalization.CultureInfo.InvariantCulture;
                serializer.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
                serializer.DateFormatHandling = DateFormatHandling.IsoDateFormat;
                serializer.DateParseHandling = DateParseHandling.DateTimeOffset;
                serializer.Serialize(file, obj);
            }
        }
    }
}
