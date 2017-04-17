using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace LargeData
{
    public static class ServerSettings
    {
        /// <summary>
        /// Maximum size of file, which can be transferred
        /// </summary>
        public static int MaxFileSize { get; set; }

        /// <summary>
        /// Maximum size of file, which can be transferred
        /// </summary>
        public static int MaxRecordsInAFile { get; set; }

        /// <summary>
        /// Temporary directory location, where all files will be created and preserved to be transferred
        /// </summary>
        public static string TemporaryLocation { get; set; }

        /// <summary>
        /// call back that will accept filters and return the final dataset to be servered
        /// </summary>
        public static Func<List<Filter>, string, DataSet> Callback { get; set; }

        /// <summary>
        /// call back that will accept filters and return the final datareader to be servered
        /// </summary>
        public static Func<List<Filter>, string, IDataReader> CallbackReader { get; set; }

        public static Action<DataSet, List<Filter>> CallbackUpload { get; set; }

        public static Action<IDataReader, List<Filter>> CallbackUploadReader { get; set; }
    }
}
