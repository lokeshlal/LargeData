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
    public class BackgroundWorkers
    {
        public static void GenerateFilesForDownload(string guid, List<Filter> filters, CancellationToken c, ICache cache)
        {
            try
            {
                // set status to inprogress
                var taskStatus = cache.Get<TaskState>(guid);
                taskStatus.Status = TaskStatus.InProgress;
                cache.Remove(guid);
                cache.Put<TaskState>(guid, taskStatus);

                // get dataset from callback
                DataSet dataset = ServerSettings.Callback(filters, guid);
                string temporaryLocation = ServerSettings.TemporaryLocation;

                // generate files
                string rootDirectory = FileHelper.CreateFiles(guid, dataset, temporaryLocation);

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
            }
        }
    }
}
