using LargeData;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace WebApi
{
    public class DataProvider
    {
        public static DataSet GetDataForDownload(List<Filter> filters, string guid)
        {
            DataSet dataSet = new DataSet();

            DataTable dt = new DataTable("Table1");
            dt.Columns.Add("Col1", typeof(string));
            dt.Columns.Add("Col2", typeof(int));
            dt.Columns.Add("Col3", typeof(DateTime));
            dt.Columns.Add("Col4", typeof(byte[]));
            dt.Columns.Add("Col5", typeof(float));
            dt.Columns.Add("Col6", typeof(decimal));
            dt.Columns.Add("Col7", typeof(Guid));
            dt.Columns.Add("Col8", typeof(DateTimeOffset));
            dt.Columns.Add("Col9", typeof(long));
            dt.Columns.Add("Col10", typeof(Boolean));
            dt.Columns.Add("Col11", typeof(byte));
            dt.Columns.Add("Col12", typeof(double));
            dt.Columns.Add("Col13", typeof(Single));


            dt.AcceptChanges();
            DataRow dr = dt.NewRow();
            dr["Col1"] = "Hello";
            dr["Col2"] = 1;
            dr["Col3"] = DateTime.Now;
            dr["Col4"] = Encoding.UTF8.GetBytes("Hello World");
            dr["Col5"] = 2.5F;
            dr["Col6"] = Convert.ToDecimal(2.99);
            dr["Col7"] = Guid.NewGuid();
            dr["Col8"] = DateTimeOffset.Now;
            dr["Col9"] = 655;
            dr["Col10"] = true;
            dr["Col11"] = Encoding.UTF8.GetBytes("Hello World")[0];
            dr["Col12"] = Convert.ToDouble(1.99);
            dr["Col13"] = Convert.ToSingle(0.99);

            dt.Rows.Add(dr);

            dataSet.Tables.Add(dt);

            return dataSet;
        }

        public static IDataReader GetDataReaderForDownload(List<Filter> filters, string guid)
        {
            DataSet dataSet = new DataSet();

            DataTable dt = new DataTable("Table1");
            dt.Columns.Add("Col1", typeof(string));
            dt.Columns.Add("Col2", typeof(int));
            dt.Columns.Add("Col3", typeof(DateTime));
            dt.Columns.Add("Col4", typeof(byte[]));
            dt.Columns.Add("Col5", typeof(float));
            dt.Columns.Add("Col6", typeof(decimal));
            dt.Columns.Add("Col7", typeof(Guid));
            dt.Columns.Add("Col8", typeof(DateTimeOffset));
            dt.Columns.Add("Col9", typeof(long));
            dt.Columns.Add("Col10", typeof(Boolean));
            dt.Columns.Add("Col11", typeof(byte));
            dt.Columns.Add("Col12", typeof(double));
            dt.Columns.Add("Col13", typeof(Single));


            dt.AcceptChanges();
            DataRow dr = dt.NewRow();
            dr["Col1"] = "Hello";
            dr["Col2"] = 1;
            dr["Col3"] = DateTime.Now;
            dr["Col4"] = Encoding.UTF8.GetBytes("Hello World");
            dr["Col5"] = 2.5F;
            dr["Col6"] = Convert.ToDecimal(2.99);
            dr["Col7"] = Guid.NewGuid();
            dr["Col8"] = DateTimeOffset.Now;
            dr["Col9"] = 655;
            dr["Col10"] = true;
            dr["Col11"] = Encoding.UTF8.GetBytes("Hello World")[0];
            dr["Col12"] = Convert.ToDouble(1.99);
            dr["Col13"] = Convert.ToSingle(0.99);

            dt.Rows.Add(dr);
            dataSet.Tables.Add(dt);

            dt = new DataTable("Table2");
            dt.Columns.Add("Col1", typeof(string));
            dt.Columns.Add("Col2", typeof(int));
            dt.Columns.Add("Col3", typeof(DateTime));
            dt.Columns.Add("Col4", typeof(byte[]));
            dt.Columns.Add("Col5", typeof(float));
            dt.Columns.Add("Col6", typeof(decimal));
            dt.Columns.Add("Col7", typeof(Guid));
            dt.Columns.Add("Col8", typeof(DateTimeOffset));
            dt.Columns.Add("Col9", typeof(long));
            dt.Columns.Add("Col10", typeof(Boolean));
            dt.Columns.Add("Col11", typeof(byte));
            dt.Columns.Add("Col12", typeof(double));
            dt.Columns.Add("Col13", typeof(Single));


            dt.AcceptChanges();
            dr = dt.NewRow();
            dr["Col1"] = "World";
            dr["Col2"] = 1;
            dr["Col3"] = DateTime.Now;
            dr["Col4"] = Encoding.UTF8.GetBytes("Hello World");
            dr["Col5"] = 2.5F;
            dr["Col6"] = Convert.ToDecimal(2.99);
            dr["Col7"] = Guid.NewGuid();
            dr["Col8"] = DateTimeOffset.Now;
            dr["Col9"] = 655;
            dr["Col10"] = true;
            dr["Col11"] = Encoding.UTF8.GetBytes("Hello World")[0];
            dr["Col12"] = Convert.ToDouble(1.99);
            dr["Col13"] = Convert.ToSingle(0.99);

            dt.Rows.Add(dr);

            dataSet.Tables.Add(dt);

            return dataSet.CreateDataReader();
        }
    }
}