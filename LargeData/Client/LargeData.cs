using LargeData.Helpers;
using LargeData.Models;
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
    /// <summary>
    /// client large data class to initiate get and send dataset from server
    /// </summary>
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
                    JsonSerializer serializer = FileHelper.JsonSerializerSettings();

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

                    FileHelper.PopulateTable(rowCollection, dt, existingTable);
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
        public async Task<bool> SendData(DataSet dataSet, List<Filter> filters, string baseUri = null, string temporaryLocation = null)
        {
            if (string.IsNullOrEmpty(baseUri))
            {
                baseUri = ClientSettings.BaseUri;
            }

            if (string.IsNullOrEmpty(temporaryLocation))
            {
                temporaryLocation = ClientSettings.TemporaryLocation;
            }

            string guid = Guid.NewGuid().ToString();

            var reader = dataSet.CreateDataReader();

            string rootDirectory = FileHelper.CreateFilesUsingReader(guid, reader, temporaryLocation);
            var files = Directory.GetFiles(rootDirectory);
            int totalFiles = files.Count();

            // zip files in batches
            var totalZipFiles = (totalFiles / 10 + (totalFiles % 10 > 0 ? 1 : 0));
            List<string> zipFileList = new List<string>();
            List<string> zipFileNameList = new List<string>();

            string randomNumber = Guid.NewGuid().ToString().Replace("-", string.Empty);
            string zipDirectoryName = "zipped" + randomNumber;
            string zipFileName = "zippedFile" + randomNumber + ".zip";
            string zipDirectory = Path.Combine(rootDirectory, zipDirectoryName);
            string zipDirectoryFormat = zipDirectory + "{0}"; ;
            var zipFileFormattemp = "zippedFile" + randomNumber + "{0}.zip";
            Directory.CreateDirectory(zipDirectory);

            string zipFile = Path.Combine(rootDirectory, zipFileName);
            string zipFileFormat = Path.Combine(rootDirectory, zipFileFormattemp);

            for (int i = 0; i < totalZipFiles; i++)
            {
                var newDirectory = string.Format(zipDirectoryFormat, i);
                var newFile = string.Format(zipFileFormat, i);
                if (!Directory.Exists(newDirectory))
                {
                    Directory.CreateDirectory(newDirectory);
                }
                for (int j = i * 10; j < ((i * 10) + 10) && j < totalFiles; j++)
                {
                    var newFileName = Path.Combine(newDirectory, Path.GetFileName(files[j]));
                    File.Move(files[j], newFileName);
                }
                ZipFile.CreateFromDirectory(newDirectory, newFile);
                zipFileNameList.Add(Path.GetFileName(newFile));
                zipFileList.Add(newFile);
                Directory.Delete(newDirectory, true);
            }

            Directory.Delete(zipDirectory, true);


            int totalFileCount = zipFileList.Count;
            int filesDownloaded = 0;

            int maxBytesInAFile = ClientSettings.MaxFileSize > 0 ? ClientSettings.MaxFileSize : 102400; // maximum size 100 KB

            string serverGuid = await RestClient.Execute<List<Filter>, string>(filters, "api/largedata/beginupload", baseUri);

            Parallel.ForEach(zipFileList, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, new Action<string>((file) =>
            {
                if (File.Exists(file))
                {
                    FileInfo fileInfo = new FileInfo(file);
                    if (fileInfo.Length <= maxBytesInAFile) // maximum size 100 KB
                    {
                        RestClient.Execute<string, bool>(serverGuid, file, "api/largedata/postfile", baseUri).GetAwaiter().GetResult();
                    }
                    else
                    {
                        // split the file and upload
                        // this most probably be the case, when uploading zip file containing project structure row
                        // as in most cases upload speed is slow and download is acceptable (and first response is receieved avoiding rest api time out
                        var baseDirectory = Path.GetDirectoryName(file);
                        List<string> packets = new List<string>();

                        FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read);

                        int noOfFiles = (int)Math.Ceiling((double)fileInfo.Length / maxBytesInAFile);
                        for (int fileCounter = 0; fileCounter < noOfFiles; fileCounter++)
                        {
                            string baseFileName = Path.GetFileNameWithoutExtension(file);
                            string Extension = Path.GetExtension(file);

                            string packetName = baseDirectory + "\\" + fileCounter.ToString() + "-" + baseFileName + ".tmp";
                            FileStream outputFile = new FileStream(packetName, FileMode.Create, FileAccess.Write);
                            int bytesRead = 0;
                            byte[] buffer = new byte[maxBytesInAFile];

                            if ((bytesRead = fs.Read(buffer, 0, maxBytesInAFile)) > 0)
                            {
                                outputFile.Write(buffer, 0, bytesRead);
                                packets.Add(packetName);
                            }
                            outputFile.Close();

                        }
                        fs.Close();
                        // remove this file from the zip file packet
                        zipFileNameList.Remove(Path.GetFileName(file));
                        List<string> packetCreated = new List<string>();
                        foreach (var packet in packets)
                        {
                            // add individual packets
                            packetCreated.Add(Path.GetFileName(packet));
                            RestClient.Execute<string, bool>(serverGuid, packet, "api/largedata/uploadfile", baseUri).GetAwaiter().GetResult();
                            // delete the packet from local folder
                            File.Delete(packet);
                        }

                        zipFileNameList.Add(string.Join("|", packetCreated.ToArray()));
                    }
                    File.Delete(file);
                }
                filesDownloaded++;
            }));

            var isProcessStarted = await RestClient.Execute<UploadModel, bool>(new UploadModel() { guid = serverGuid, files = zipFileNameList }, "api/largedata/processuploadedfiles", baseUri);

            DateTime dtWait = DateTime.Now;
            bool isServerProcessCompleted = false;
            while (DateTime.Now <= dtWait.AddSeconds(60))
            {
                isServerProcessCompleted = await RestClient.Execute<string, bool>(serverGuid, "api/largedata/getuploadprocessstatus", baseUri);
            }
            if (!isServerProcessCompleted)
            {
                // server time outs. call end download
                await RestClient.Execute<string, string>(guid, "api/largedata/endupload", baseUri);
            }
            // TODO
            return isServerProcessCompleted;
        }
    }
}
