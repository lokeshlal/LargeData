using LargeData.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Hosting;
using System.Web.Http;

namespace LargeData.Controllers
{
    [Route("api/largedata/{action}")]
    public class LargeDataController : ApiController
    {
        private ICache cache = new Cache();

        #region methods to download the dataset
        /// <summary>
        /// Returns Guid for the current executing task
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        [HttpPost]
        public Task<string> BeginDownload([FromBody] List<Filter> filters)
        {
            string guid = Guid.NewGuid().ToString();


            cache.Put(guid, new TaskState()
            {
                Guid = guid,
                HeaderInfo = string.Empty,
                FilesToTransfer = new List<string>(),
                OriginalFileList = new List<string>(),
                Status = TaskStatus.Submitted
            });

            // start the callback in background and let it complete the data creation process
            // if this process exceeds a time limit of 1 minutes, then move this process to a seperate process (like a windows service)
            HostingEnvironment.QueueBackgroundWorkItem(c =>
            {
                BackgroundWorkers.GenerateFilesForDownload(guid, filters, c, cache);
            });

            return Task.FromResult(guid);
        }

        /// <summary>
        /// This method will be repeatedly called, to check the completion of server side process
        /// </summary>
        /// <param name="guid"></param>
        /// <returns>List of files to be downloaded</returns>
        [HttpPost]
        public List<string> GetFilesListToDownload([FromBody] string guid)
        {
            var response = new List<string>();
            var taskState = cache.Get<TaskState>(guid);
            if (taskState != null && !(taskState != null && taskState.Status == TaskStatus.Failed))
            {
                if (taskState.Status == TaskStatus.Completed) response = taskState.FilesToTransfer;
            }
            else
            {
                cache.Remove(guid);
                throw new Exception("Process failed. Please try again.");
            }
            return response;
        }

        /// <summary>
        /// download individual files for the process associated with the guid
        /// </summary>
        /// <param name="guid">process identifier</param>
        /// <param name="fileName">file to be downloaded</param>
        /// <returns></returns>
        [HttpPost]
        public HttpResponseMessage DownloadFile([FromBody] DownloadFileModel downloadFileModel)
        {
            string filePath = string.Empty;
            try
            {
                string guid = downloadFileModel.guid;
                string fileName = downloadFileModel.fileName;
                string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
                string rootDirectory = Path.Combine(ServerSettings.TemporaryLocation, taskDirectoryName);
                filePath = Path.Combine(rootDirectory, fileName);

                byte[] fileBytes = File.ReadAllBytes(filePath);

                var result = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(fileBytes)
                };
                return result;
            }
            finally
            {
                if (!string.IsNullOrWhiteSpace(filePath))
                    File.Delete(filePath);
            }
        }

        /// <summary>
        /// Marks the end of download for a guid
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        [HttpPost]
        public HttpResponseMessage EndDownload([FromBody] string guid)
        {
            cache.Remove(guid);

            // clear all files in the folder, if any remaining
            string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
            string rootDirectory = Path.Combine(ServerSettings.TemporaryLocation, taskDirectoryName);
            Directory.Delete(rootDirectory, true);

            var result = new HttpResponseMessage(HttpStatusCode.OK);
            return result;
        }
        #endregion

        #region methods to upload the dataset
        [HttpPost]
        public Task<string> BeginUpload([FromBody] List<Filter> filters)
        {
            string guid = Guid.NewGuid().ToString();

            cache.Put(guid, new TaskState()
            {
                Guid = guid,
                HeaderInfo = string.Empty,
                FilesToTransfer = new List<string>(),
                OriginalFileList = new List<string>(),
                Status = TaskStatus.Submitted,
                Type = TaskType.Upload,
                Filters = filters // adding filters, as this will help in distinguishing kind of data upload, for example: upload for a specific user, or for a specific functionality, etc
            });

            // create upload path for this upload
            string rootDirectory = GetRootDirectoryForGuid(guid);
            Directory.CreateDirectory(rootDirectory);

            return Task.FromResult(guid);
        }

        private static string GetRootDirectoryForGuid(string guid)
        {
            string temporaryLocation = ServerSettings.TemporaryLocation;
            string taskDirectoryName = string.Format("f{0}", guid.Replace("-", string.Empty));
            string rootDirectory = Path.Combine(temporaryLocation, taskDirectoryName);
            return rootDirectory;
        }

        [HttpPost]
        public Task<bool> PostFile()
        {
            string guid = JsonConvert.DeserializeObject<string>(Request.Headers.GetValues("objectValue").FirstOrDefault());
            string rootDirectory = GetRootDirectoryForGuid(guid);
            HttpFileCollection hfc = System.Web.HttpContext.Current.Request.Files;
            if (hfc.Count > 0)
            {
                HttpPostedFile file = hfc[0];
                var uploadedFilePath = Path.Combine(rootDirectory, file.FileName);
                file.SaveAs(uploadedFilePath);
            }
            return Task.FromResult(true);
        }

        [HttpPost]
        public Task<bool> ProcessUploadedFiles(UploadModel model)
        {
            string guid = model.guid;
            List<string> files = model.files;

            HostingEnvironment.QueueBackgroundWorkItem(c =>
            {
                // process the uploaded files
                BackgroundWorkers.ProcessUploadedFiles(guid, files, cache);
            });
            return Task.FromResult(true);
        }

        [HttpPost]
        public Task<bool> GetUploadProcessStatus([FromBody] string guid)
        {
            var response = false;
            var taskState = cache.Get<TaskState>(guid);
            if (taskState != null && !(taskState != null && taskState.Status == TaskStatus.Failed))
            {
                if (taskState.Status == TaskStatus.Completed) response = true;
            }
            else
            {
                cache.Remove(guid);
                throw new Exception("Process failed. Please try again.");
            }
            return Task.FromResult(response);
        }

        [HttpPost]
        public Task<bool> EndUpload([FromBody] string guid)
        {
            cache.Remove(guid);
            // clear all files in the folder, if any remaining
            string rootDirectory = GetRootDirectoryForGuid(guid);
            Directory.Delete(rootDirectory, true);
            return Task.FromResult(true);
        }
        #endregion
    }
}
