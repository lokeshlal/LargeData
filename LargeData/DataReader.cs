using LargeData.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LargeData
{
    public class DataReader : IDataReader
    {

        private bool _isClosed = false;
        private DataTable schema = default(DataTable);
        private object[] _data;


        private string zipDirectory = string.Empty;
        private string rootDirectory = string.Empty;
        IOrderedEnumerable<string> dataFile;
        int totalFileCount;
        int currentOrder = 1;
        int maximumOrderId;

        int currentFileIndex = -1;
        int CurrentFileRowIndex = 0;
        DataTable cachedRows;

        private bool InternalNextResult()
        {
            if (currentOrder <= maximumOrderId)
            {
                return true;
            }
            return false;
        }

        private bool InternalRead()
        {
            if (cachedRows != null && CurrentFileRowIndex < cachedRows.Rows.Count)
            {
                _data = cachedRows.Rows[CurrentFileRowIndex].ItemArray;
                CurrentFileRowIndex++;
                return true;
            }
            else
            {
                CurrentFileRowIndex = 0;
                currentFileIndex++;
                if (currentFileIndex >= dataFile.Count())
                {
                    _data = null;
                    cachedRows = null;
                    return false;
                }
                string file = dataFile.ToArray()[currentFileIndex];
                int currentFileOrder = Convert.ToInt32(Path.GetFileName(file).Split(new char[] { '-' })[0]);
                if (currentFileOrder != currentOrder)
                {
                    currentFileIndex--;
                    _data = null;
                    cachedRows = null;
                    return false;
                    // nextresult is required to be called to move to next table set
                }
                StreamReader sr = new StreamReader(file);
                using (JsonTextReader jsonReader = new JsonTextReader(sr))
                {
                    JsonSerializer serializer = FileHelper.JsonSerializerSettings();

                    List<Dictionary<string, FieldValue>> rowCollection = serializer.Deserialize<List<Dictionary<string, FieldValue>>>(jsonReader);

                    cachedRows = new DataTable();

                    FileHelper.PopulateTable(rowCollection, cachedRows);

                    schema = cachedRows.CreateDataReader().GetSchemaTable();
                    if (CurrentFileRowIndex < cachedRows.Rows.Count)
                    {
                        _data = cachedRows.Rows[CurrentFileRowIndex].ItemArray;
                        CurrentFileRowIndex++;
                        return true;
                    }
                    else
                    {
                        _data = null;
                        cachedRows = null;
                        return false;
                    }
                }
            }
        }


        public DataReader(string zipDirectory, string rootDirectory)
        {
            this.rootDirectory = rootDirectory;
            this.zipDirectory = zipDirectory;
            // get all files in zip directory
            this.dataFile = Directory.GetFiles(zipDirectory)
                .Where(f => f.EndsWith(".json"))
                .OrderBy(f => Convert.ToInt32(Path.GetFileName(f).Split(new char[] { '-' })[0]));
            // total file count
            this.totalFileCount = dataFile.Count();

            maximumOrderId = Directory.GetFiles(zipDirectory)
               .Where(f => f.EndsWith(".json"))
               .Select(f => Convert.ToInt32(Path.GetFileName(f).Split(new char[] { '-' })[0]))
               .OrderByDescending(f => f).First();


            _isClosed = false;
        }

        public object this[int i]
        {
            get
            {
                return GetValue(i);
            }
        }

        public object this[string name]
        {
            get
            {
                return GetValue(GetOrdinal(name));
            }
        }

        public int Depth
        {
            get
            {
                if (this.IsClosed)
                {
                    throw new Exception("Depth");
                }
                return 0;
            }
        }

        public bool IsClosed
        {
            get
            {
                return _isClosed;
            }
        }

        // not sure how this can be delivered, untill unless we get this information from the server in a header file
        // something like 
        // Table1->10 Records
        // Table2-> 99 Records
        public int RecordsAffected => throw new NotImplementedException();

        public int FieldCount
        {
            get
            {
                return schema.Rows.Count;
            }
        }

        public void Close()
        {
            // close all associated objects
            // such as json reader, file streams, delete all files
            Directory.Delete(rootDirectory, true);
        }

        public void Dispose()
        {
            // call close
            Close();
        }

        public bool GetBoolean(int i)
        {
            DataNullCheck();
            return Convert.ToBoolean(_data[i]);
        }

        private void DataNullCheck()
        {
            if (_data == null)
                throw new ArgumentNullException("No record found");
        }

        public byte GetByte(int i)
        {
            DataNullCheck();
            return Convert.ToByte(_data[i]);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            return schema.Columns[i].DataType.FullName;
        }

        public DateTime GetDateTime(int i)
        {
            DataNullCheck();
            return Convert.ToDateTime(_data[i]);
        }

        public decimal GetDecimal(int i)
        {
            DataNullCheck();
            return Convert.ToDecimal(_data[i]);
        }

        public double GetDouble(int i)
        {
            DataNullCheck();
            return Convert.ToDouble(_data[i]);
        }

        public Type GetFieldType(int i)
        {
            return schema.Columns[i].DataType;
        }

        public float GetFloat(int i)
        {
            DataNullCheck();
            return Convert.ToSingle(_data[i]);
        }

        public Guid GetGuid(int i)
        {
            DataNullCheck();
            return new Guid(Convert.ToString(_data[i]));
        }

        public short GetInt16(int i)
        {
            DataNullCheck();
            return Convert.ToInt16(_data[i]);
        }

        public int GetInt32(int i)
        {
            DataNullCheck();
            return Convert.ToInt32(_data[i]);
        }

        public long GetInt64(int i)
        {
            DataNullCheck();
            return Convert.ToInt64(_data[i]);
        }

        public string GetName(int i)
        {
            return cachedRows.Columns[i].ColumnName;
        }

        public int GetOrdinal(string name)
        {
            if (cachedRows == null) throw new ArgumentNullException("_data is null");
            return cachedRows.Columns[name].Ordinal;
        }

        public DataTable GetSchemaTable()
        {
            return schema;
        }

        public string GetString(int i)
        {
            DataNullCheck();
            return Convert.ToString(_data[i]);
        }

        public object GetValue(int i)
        {
            return _data[i];
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            currentOrder++;
            return InternalNextResult();
        }

        public bool Read()
        {
            // read will read the records one by one
            return InternalRead();
        }
    }
}
