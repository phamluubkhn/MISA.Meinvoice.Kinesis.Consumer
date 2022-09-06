using Amazon.Kinesis.ClientLibrary;
using System;
using System.Collections.Generic;
using System.Text;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class RecordProcessorEntity 
    {
        public int Id { get; set; }
        public string Consumer { get; set; }

        public Record Record { get; set; }

        public SyncErrorExtraData ExtraData { get; set; }

        public string ErrorDetail { get; set; }

        public int ErrorLevel { get; set; }

        public DateTime ErrorTime { get; set; } = DateTime.Now;
    }
    public class BatchRecordProcessorEntity
    {
        public int Id { get; set; }
        public string Consumer { get; set; }

        public List<Record> Record { get; set; }

        public SyncErrorExtraData ExtraData { get; set; }

        public string ErrorDetail { get; set; }

        public int ErrorLevel { get; set; }

        public DateTime ErrorTime { get; set; } = DateTime.Now;
    }


    public class RecordSyncError
    {
        public string Id { get; set; }
        public string Consumer { get; set; }

        public byte[] RecordData { get; set; }
    }


    public class SyncErrorExtraData {
        public string SqlState { get; set; }
        public string SqlStateMessage { get; set; }
    }

    public class MysqlState
    {
        public const string customer_status_empty = "45001";
        public const string customer_status_invalid = "45002";
    }

    public class MysqlStateMessage
    {
        public const string customer_status_empty = "customer_status_empty";
        public const string customer_status_invalid = "customer_status_invalid";
    }

    public class KCLApplication
    {
        public const string Customer = "customer";
        public const string CustomerBankAccount = "customer_bank_account";
        public const string Company = "company";
        public const string Transaction = "transaction";
        public const string Pl01GTGT = "Pl01gtgt";
        public const string Currency = "Currency";
    }

    public class SyncDataErrorLevel
    {
        public const int OtherException = 1;
        public const int MysqlException = 2;
        public const int RecordException = 3;
        public const int StopConsumer = 4;
        public const int BatchInsertException = 5;
        public const int BatchTransactionInsertException = 6;
    }

    public class KinesisStreamEvent
    {
        public const string Insert = "INSERT";
        public const string Modify = "MODIFY";
        public const string Remove = "REMOVE";
    }
}
