using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LargeData.Client
{
    public class LargeData
    {
        private static readonly object lockObj = new object();

        /// <summary>
        /// Get the large data set from the rest api
        /// </summary>
        /// <param name="filters">Filters to filter the data</param>
        /// <param name="baseUri">base uri</param>
        /// <param name="temporaryLocation">temporary location where data will be copied, preferrable is to keep this location encrypted (may be EFS) to make sure all data during transit is secure</param>
        /// <returns>DataSet returned from server</returns>
        public async Task<DataSet> GetData(List<Filter> filters, string baseUri = null, string temporaryLocation = null)
        {
            DataSet dataSet = new DataSet();
            if (string.IsNullOrEmpty(baseUri))
            {
                baseUri = ClientSettings.BaseUri;
            }

            if (string.IsNullOrEmpty(temporaryLocation))
            {
                temporaryLocation = ClientSettings.TemporaryLocation;
            }

            // rest call

            string guid = await RestClient.Execute<List<Filter>, string>(filters, "api/largedata/begindownload", baseUri);

            // check for 60 seconds for server to finish creating data files
            DateTime dtWait = DateTime.Now;
            bool isServerProcessCompleted = false;
            var listOfFiles = new List<string>();
            while (DateTime.Now <= dtWait.AddSeconds(60))
            {
                listOfFiles = await RestClient.Execute<string, List<string>>(guid, "api/largedata/getfileslisttodownload", baseUri);
                if (listOfFiles != null && listOfFiles.Count > 0)
                {
                    isServerProcessCompleted = true;
                    break;
                }
            }
            if (!isServerProcessCompleted)
            {
                // server time outs. call end download
                await RestClient.Execute<string, string>(guid, "api/largedata/enddownload", baseUri);
            }

            string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
            string rootDirectory = Path.Combine(temporaryLocation, taskDirectoryName);
            string zipDirectory = Path.Combine(rootDirectory, "zipped");
            Directory.CreateDirectory(zipDirectory);


            Parallel.ForEach(listOfFiles, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, new Action<string>((file) =>
            {
                var fileByte = RestClient.ExecuteForByteArray<DownloadFileModel>(new DownloadFileModel() { guid = guid, fileName = file }, "api/largedata/downloadfile", baseUri).GetAwaiter().GetResult();
                lock (lockObj)
                {
                    string zipFileLocal = Path.Combine(rootDirectory, file);
                    File.WriteAllBytes(zipFileLocal, fileByte);
                    ZipFile.ExtractToDirectory(zipFileLocal, zipDirectory);
                    File.Delete(zipFileLocal);
                }
            }));

            var dataFile = Directory.GetFiles(zipDirectory)
                    .Where(f => f.EndsWith(".json"))
                    .OrderBy(f => Convert.ToInt32(Path.GetFileName(f).Split(new char[] { '-' })[0]));

            int totalFileCount = dataFile.Count();
            // keep currentOrder file, to maintain the order of returned dataset
            int currentOrder = 1;
            foreach (var file in dataFile)
            {

                string fileName = Path.GetFileNameWithoutExtension(file);
                string[] fileAttributes = fileName.Split(new char[] { '-' });

                int order = Convert.ToInt32(fileAttributes[0]);
                if (currentOrder < order)
                {
                    // if no table is being sent from server, then add an empty table to keep the same orderA
                    DataTable emptyTable = new DataTable();
                    emptyTable.TableName = string.Format("Empty-Table-{0}", currentOrder);
                    dataSet.Tables.Add(emptyTable);
                    currentOrder++;
                }
                else
                {
                    currentOrder++;
                }
                string tableName = fileAttributes[1];
                int count = Convert.ToInt32(fileAttributes[2]);
                bool isContainIdentityColumn = Convert.ToBoolean(fileAttributes[3]);


                StreamReader sr = new StreamReader(file);
                using (JsonTextReader jsonReader = new JsonTextReader(sr))
                {
                    JsonSerializer serializer = new JsonSerializer();
                    serializer.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
                    serializer.DateFormatHandling = DateFormatHandling.IsoDateFormat;
                    serializer.DateParseHandling = DateParseHandling.DateTimeOffset;
                    serializer.Culture = System.Globalization.CultureInfo.InvariantCulture;

                    List<Dictionary<string, FieldValue>> rowCollection = serializer.Deserialize<List<Dictionary<string, FieldValue>>>(jsonReader);

                    DataTable dt = new DataTable();
                    bool existingTable = false;
                    if (dataSet.Tables.Contains(tableName))
                    {
                        dt = dataSet.Tables[tableName];
                        existingTable = true;
                    }
                    else
                    {
                        dt = new DataTable(tableName);
                        dataSet.Tables.Add(dt);
                    }

                    List<string> fields = new List<string>();
                    List<string> uTablefield = new List<string>();
                    List<string> uTableUpdatedField = new List<string>();


                    bool firstRun = true;
                    foreach (var row in rowCollection)
                    {
                        if (firstRun && !existingTable) // only for new tables
                        {
                            foreach (var key in row.Keys)
                            {

                                string columnNameForQuery = string.Format("[{0}]", key);
                                fields.Add(columnNameForQuery);
                                uTablefield.Add(string.Format("U.{0}", columnNameForQuery));
                                if (columnNameForQuery.ToLower() != "[id]")
                                {
                                    uTableUpdatedField.Add(string.Format("T.{0} = U.{0}", columnNameForQuery));
                                }

                                Type dataType = typeof(int);
                                switch (Convert.ToString(row[key].StringValue))
                                {
                                    case "System.Int32": dataType = typeof(int); break;
                                    case "System.Int64": dataType = typeof(long); break;
                                    case "System.DateTimeOffset": dataType = typeof(DateTimeOffset); break;
                                    case "System.DateTime": dataType = typeof(DateTime); break;
                                    case "System.String": dataType = typeof(string); break;
                                    case "System.Boolean": dataType = typeof(bool); break;
                                    case "System.Byte[]": dataType = typeof(byte[]); break;
                                    case "System.Guid": dataType = typeof(Guid); break;
                                    case "System.Decimal":
                                    case "System.Single":
                                    case "System.Double":
                                        dataType = typeof(decimal); break;
                                }
                                var dataColumn = dt.Columns.Add(key, dataType);
                                dataColumn.AllowDBNull = true; // allowing null
                            }
                            firstRun = !firstRun;
                        }
                        else
                        {
                            var newRow = dt.NewRow();
                            foreach (var key in row.Keys)
                            {
                                var value = row[key];
                                if (value == null)
                                {
                                    newRow[key] = DBNull.Value;
                                    continue;
                                }

                                if (dt.Columns.Contains(key))
                                {
                                    // use switch instead of multiple if conditions
                                    if (dt.Columns[key].DataType == typeof(byte[]))
                                    {
                                        if (value.ByteValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.ByteValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(Guid))
                                    {
                                        if (value.GuidValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.GuidValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(bool))
                                    {
                                        if (value.BoolValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.BoolValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(Int32))
                                    {
                                        if (value.IntValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.IntValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(Int64))
                                    {
                                        if (value.LongValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.LongValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(DateTimeOffset))
                                    {
                                        if (value.DateTimeOffsetValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.DateTimeOffsetValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(DateTime))
                                    {
                                        if (value.DateTimeValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.DateTimeValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(decimal)
                                        || dt.Columns[key].DataType == typeof(double)
                                        || dt.Columns[key].DataType == typeof(Single)
                                        || dt.Columns[key].DataType == typeof(float))
                                    {
                                        if (value.DecimalValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.DecimalValue;
                                        }
                                    }
                                    else if (dt.Columns[key].DataType == typeof(string))
                                    {
                                        if (value.StringValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = value.StringValue;
                                        }
                                    }
                                    else
                                    {
                                        if (value.ByteValue == null)
                                        {
                                            newRow[key] = DBNull.Value;
                                        }
                                        else
                                        {
                                            newRow[key] = row[key];
                                        }
                                    }
                                }
                            }
                            dt.Rows.Add(newRow);
                        }
                    }
                }
            }
            Directory.Delete(rootDirectory, true);
            return dataSet;
        }

        /// <summary>
        /// Get the large data set from the rest api
        /// </summary>
        /// <param name="filters">Filters to filter the data</param>
        /// <param name="baseUri">base uri</param>
        /// <param name="temporaryLocation">temporary location where data will be copied, preferrable is to keep this location encrypted (may be EFS) to make sure all data during transit is secure</param>
        /// <returns>DataSet returned from server</returns>
        public async Task<IDataReader> GetDataReaders(List<Filter> filters, string baseUri = null, string temporaryLocation = null)
        {
            DataSet dataSet = new DataSet();
            if (string.IsNullOrEmpty(baseUri))
            {
                baseUri = ClientSettings.BaseUri;
            }

            if (string.IsNullOrEmpty(temporaryLocation))
            {
                temporaryLocation = ClientSettings.TemporaryLocation;
            }

            // rest call

            string guid = await RestClient.Execute<List<Filter>, string>(filters, "api/largedata/begindownload", baseUri);

            // check for 60 seconds for server to finish creating data files
            DateTime dtWait = DateTime.Now;
            bool isServerProcessCompleted = false;
            var listOfFiles = new List<string>();
            while (DateTime.Now <= dtWait.AddSeconds(60))
            {
                listOfFiles = await RestClient.Execute<string, List<string>>(guid, "api/largedata/getfileslisttodownload", baseUri);
                if (listOfFiles != null && listOfFiles.Count > 0)
                {
                    isServerProcessCompleted = true;
                    break;
                }
            }
            if (!isServerProcessCompleted)
            {
                // server time outs. call end download
                await RestClient.Execute<string, string>(guid, "api/largedata/enddownload", baseUri);
            }

            string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
            string rootDirectory = Path.Combine(temporaryLocation, taskDirectoryName);
            string zipDirectory = Path.Combine(rootDirectory, "zipped");
            Directory.CreateDirectory(zipDirectory);


            Parallel.ForEach(listOfFiles, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, new Action<string>((file) =>
            {
                var fileByte = RestClient.ExecuteForByteArray<DownloadFileModel>(new DownloadFileModel() { guid = guid, fileName = file }, "api/largedata/downloadfile", baseUri).GetAwaiter().GetResult();
                lock (lockObj)
                {
                    string zipFileLocal = Path.Combine(rootDirectory, file);
                    File.WriteAllBytes(zipFileLocal, fileByte);
                    ZipFile.ExtractToDirectory(zipFileLocal, zipDirectory);
                    File.Delete(zipFileLocal);
                }
            }));

            return new DataReader(zipDirectory, rootDirectory); // dataSet;
        }

        /// <summary>
        /// send dataset to the server
        /// </summary>
        /// <param name="dataSet">dataset to be sent</param>
        /// <returns>true, if succeed, else false</returns>
        public async Task<bool> SendData(DataSet dataSet, string baseUri = null, string temporaryLocation = null)
        {
            bool result = false;

            if (string.IsNullOrEmpty(baseUri))
            {
                baseUri = ClientSettings.BaseUri;
            }

            if (string.IsNullOrEmpty(temporaryLocation))
            {
                temporaryLocation = ClientSettings.TemporaryLocation;
            }


            // TODO
            return result;
        }
    }
}
