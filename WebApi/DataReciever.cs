using LargeData;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Web;

namespace WebApi
{
    public class DataReciever
    {
        public static void AcceptDataSet(DataSet dataSet, List<Filter> filters)
        {
            var tableCount = dataSet.Tables.Count;
        }

        public static void AcceptDataReader(IDataReader reader, List<Filter> filters)
        {
            while (reader.Read())
            {
                Debug.WriteLine(string.Format("{0}", reader["Col1"]));
            }

            while (reader.NextResult())
            {
                Debug.WriteLine("Next result");
                while (reader.Read())
                {
                    Debug.WriteLine(string.Format("{0}", reader["Col1"]));
                }
            }
        }
    }
}