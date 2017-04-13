using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LargeData
{
    public class FieldValue
    {
        public int? IntValue { get; set; }
        public long? LongValue { get; set; }
        public bool? BoolValue { get; set; }
        public decimal? DecimalValue { get; set; }
        public byte[] ByteValue { get; set; }
        public string StringValue { get; set; }
        public Guid? GuidValue { get; set; }
        public DateTimeOffset? DateTimeOffsetValue { get; set; }
        public DateTime? DateTimeValue { get; set; }
    }
}
