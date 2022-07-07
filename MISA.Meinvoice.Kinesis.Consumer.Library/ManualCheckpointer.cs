using Amazon.Kinesis.ClientLibrary;
using System;
using System.Collections.Generic;
using System.Text;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class ManualCheckpointer
    {
        public Checkpointer Checkpointer { get; set; }
        public bool IsChecked { get; set; }
    }
}
