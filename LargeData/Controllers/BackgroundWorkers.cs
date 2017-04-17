using LargeData.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LargeData.Controllers
{
    /// <summary>
    /// Background worker class to process upload and download information
    /// if this process starts taking time, then initiate this in a new process or create a windows service
    /// Can be created a separate executabe and process class be used[-
    /// </summary>
    public class BackgroundWorkers
    {
        private static readonly object lockObj = new object();

        public static void GenerateFilesForDownload(string guid, List<Filter> filters, CancellationToken c, ICache cache)
        {

            try
            {
                // set status to inprogress
                var taskStatus = cache.Get<TaskState>(guid);
                taskStatus.Status = TaskStatus.InProgress;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);

                string temporaryLocation = ServerSettings.TemporaryLocation;
                string rootDirectory = string.Empty;

                if (ServerSettings.Callback == null)
                {
                    // assuming CallbackReader function is set, otherwise an exception will be thrown
                    IDataReader reader = ServerSettings.CallbackReader(filters, guid);
                    // generate files
                    rootDirectory = FileHelper.CreateFilesUsingReader(guid, reader, temporaryLocation);
                }
                else
                {
                    // get dataset from callback
                    DataSet dataset = ServerSettings.Callback(filters, guid);
                    rootDirectory = FileHelper.CreateFiles(guid, dataset, temporaryLocation);
                }
                var files = Directory.GetFiles(rootDirectory);
                int totalFiles = files.Count();

                // zip files in batches
                var totalZipFiles = (totalFiles / 10 + (totalFiles % 10 > 0 ? 1 : 0));
                List<string> zipFileList = new List<string>();

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
                    zipFileList.Add(Path.GetFileName(newFile));
                    Directory.Delete(newDirectory, true);
                }

                // delete the redundant files
                Directory.Delete(zipDirectory, true);

                // set status to completed
                taskStatus = cache.Get<TaskState>(guid);
                taskStatus.FilesToTransfer = zipFileList;
                taskStatus.Status = TaskStatus.Completed;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);
            }
            catch (Exception ex)
            {
                // in event of exception set status to failed and set the exception in status
                var taskStatus = cache.Get<TaskState>(guid);
                taskStatus.Exception = ex.Message;
                taskStatus.Status = TaskStatus.Failed;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);
                throw ex;
            }
        }

        internal static void ProcessUploadedFiles(string guid, List<string> filesToProcess, ICache cache)
        {
            try
            {
                // set status to inprogress
                var taskStatus = cache.Get<TaskState>(guid);

                List<Filter> filters = taskStatus.Filters;

                taskStatus.Status = TaskStatus.InProgress;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);

                string temporaryLocation = ServerSettings.TemporaryLocation;
                string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
                string rootDirectory = Path.Combine(temporaryLocation, taskDirectoryName);

                List<string> filesMerged = new List<string>();
                foreach (var file in filesToProcess)
                {
                    if (!file.Contains("|"))
                    {
                        // single unit file, add to list
                        filesMerged.Add(file);
                    }
                    else
                    {
                        // splitted files
                        var splittedFilesList = file.Split(new char[] { '|' });
                        if (splittedFilesList.Count() > 0)
                        {
                            var fileName = Path.GetFileNameWithoutExtension(splittedFilesList[0]).Split(new char[] { '-' })[1] + ".zip";
                            var uploadedFilePath = Path.Combine(rootDirectory, fileName);

                            var outPutFile = new FileStream(uploadedFilePath, FileMode.OpenOrCreate, FileAccess.Write);

                            var dataFiles = splittedFilesList
                                .OrderBy(f => Convert.ToInt32(Path.GetFileName(f).Split(new char[] { '-' })[0]));

                            foreach (var tempFile in dataFiles)
                            {
                                string dataFile = Path.GetFileName(tempFile);
                                int bytesRead = 0;
                                byte[] buffer = new byte[1024];
                                var dataFilePath = Path.Combine(rootDirectory, dataFile);
                                FileStream inputTempFile = new FileStream(dataFilePath, FileMode.OpenOrCreate, FileAccess.Read);

                                while ((bytesRead = inputTempFile.Read(buffer, 0, 1024)) > 0)
                                    outPutFile.Write(buffer, 0, bytesRead);

                                inputTempFile.Close();
                                File.Delete(dataFilePath);

                                outPutFile.Flush();
                            }
                            outPutFile.Close();
                            filesMerged.Add(fileName);
                        }
                    }
                }

                string zipDirectory = Path.Combine(rootDirectory, "zipped");
                Directory.CreateDirectory(zipDirectory);

                Parallel.ForEach(filesMerged, new ParallelOptions() { MaxDegreeOfParallelism = System.Environment.ProcessorCount * 2 }, new System.Action<string>((file) =>
                {
                    var filePath = Path.Combine(rootDirectory, file);
                    lock (lockObj)
                    {
                        ZipFile.ExtractToDirectory(filePath, zipDirectory);
                    }
                    File.Delete(filePath);
                }));

                if (ServerSettings.CallbackUpload != null)
                {
                    DataSet dataSet = new DataSet();
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
                    ServerSettings.CallbackUpload(dataSet, filters);
                }
                else
                {
                    ServerSettings.CallbackUploadReader(new DataReader(zipDirectory, rootDirectory), filters);
                }

                // delete the redundant files
                Directory.Delete(zipDirectory, true);

                // set status to completed
                taskStatus = cache.Get<TaskState>(guid);
                taskStatus.Status = TaskStatus.Completed;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);
            }
            catch (Exception ex)
            {
                // in event of exception set status to failed and set the exception in status
                var taskStatus = cache.Get<TaskState>(guid);
                taskStatus.Exception = ex.Message;
                taskStatus.Status = TaskStatus.Failed;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);
                throw ex;
            }
        }
    }
}
