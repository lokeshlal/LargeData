using LargeData;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LD = LargeData.Client;

namespace Client
{
    class Program
    {
        static void Main(string[] args)
        {
            LD.LargeData largeData = new LD.LargeData();
            DataSet ds = largeData.GetData(new List<Filter>(), "http://localhost:55953/", @"E:\Projects\LargeData\Client\bin\Debug").GetAwaiter().GetResult();
            foreach (DataColumn dc in ds.Tables[0].Columns)
            {
                Console.WriteLine(dc.ColumnName);
            }
            Console.WriteLine();
            foreach (DataRow dr in ds.Tables[0].Rows)
            {
                Console.WriteLine(string.Format("{0}-{1}-{2}", dr[0], dr[1], dr[2]));
            }
            Console.ReadLine();
        }
    }
}
