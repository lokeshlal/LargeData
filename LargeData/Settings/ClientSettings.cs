using System;
using System.Collections.Generic;
using System.Data;

namespace LargeData
{
    public class ClientSettings
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
        /// Base uri for remote rest APIs
        /// </summary>
        public static string BaseUri { get; set; }

        /// <summary>
        /// call back that will accept filters and return the final dataset to be servered
        /// </summary>
        public static Func<List<Filter>, string, DataSet> Callback { get; set; }
    }
}
