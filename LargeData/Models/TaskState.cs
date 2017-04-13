using System.Collections.Generic;

namespace LargeData
{
    public class TaskState
    {
        public string Guid { get; set; }
        public string HeaderInfo { get; set; }
        public string Exception { get; set; }
        public List<string> FilesToTransfer { get; set; }
        public List<string> OriginalFileList { get; set; }
        public TaskStatus Status { get; set; }
    }
}
