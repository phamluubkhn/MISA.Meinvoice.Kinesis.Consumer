using System;
using System.Collections.Generic;
using System.Text;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    [AttributeUsage(AttributeTargets.Property)]
    public class KinesisDataFieldAttribute : Attribute
    {
        private KinesisDataFieldType dataFieldType;

        public KinesisDataFieldAttribute(KinesisDataFieldType dataFieldType)
        {
            this.dataFieldType = dataFieldType;

        }

        public virtual KinesisDataFieldType DataFieldType
        {
            get { return dataFieldType; }
        }
    }


    public enum KinesisDataFieldType
    {
        StringType = 1,
        IntegerType = 2,
        DatetimeType = 3,
        DecimalType = 4,
    }
}
