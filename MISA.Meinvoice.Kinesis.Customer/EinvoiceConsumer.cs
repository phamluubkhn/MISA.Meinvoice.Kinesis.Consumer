using Amazon.Kinesis.ClientLibrary;
using System;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.Json;
using System.Data;
using Microsoft.Extensions.Configuration;
using MISA.Meinvoice.Kinesis.Consumer.Library;
using Microsoft.Extensions.DependencyInjection;

namespace MISA.Meinvoice.Kinesis
{
    public class EinvoiceRecordProcessor : IShardRecordProcessor
    {
        private static string configDB = ConsumerConfig.mysqlDbConfig;

        private static string applicationName = ConsumerConfig.applicationName;

        private static int maxErrorCountToShutDown = ConsumerConfig.maxErrorCountToShutDown;

        /// <value>The time to wait before this record processor
        /// reattempts either a checkpoint, or the processing of a record.</value>
        private static readonly TimeSpan Backoff = TimeSpan.FromSeconds(30);


        /// <value>The interval this record processor waits between
        /// doing two successive checkpoints.</value>
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMinutes(3);

        /// <value>The maximum number of times this record processor retries either
        /// a failed checkpoint, or the processing of a record that previously failed.</value>
        private static readonly int NumRetries = 3;

        /// <value>The shard ID on which this record processor is working.</value>
        private string _kinesisShardId;

        /// <value>The next checkpoint time expressed in milliseconds.</value>
        private DateTime _nextCheckpointTime = DateTime.UtcNow;

        /// <summary>
        /// This method is invoked by the Amazon Kinesis Client Library before records from the specified shard
        /// are delivered to this RecordProcessor.
        /// </summary>
        /// <param name="input">
        /// InitializationInput containing information such as the name of the shard whose records this
        /// SampleRecordProcessor will process.
        /// </param>
        public void Initialize(InitializationInput input)
        {
            Console.Error.WriteLine("Initializing record processor for shard: " + input.ShardId);
            this._kinesisShardId = input.ShardId;
        }

        /// <summary>
        /// This method processes the given records and checkpoints using the given checkpointer.
        /// </summary>
        /// <param name="input">
        /// ProcessRecordsInput that contains records, a Checkpointer and contextual information.
        /// </param>
        public void ProcessRecords(ProcessRecordsInput input)
        {
            Console.Error.WriteLine("ProcessRecords Start v4 - Record count " + input.Records.Count);
            Console.Error.WriteLine("ProcessRecords Start v4 time" + DateTime.Now);
            // Process records and perform all exception handling.
            ProcessRecords(input.Records);
            Console.Error.WriteLine("ProcessRecords End v4 time" + DateTime.Now);
            // Checkpoint once every checkpoint interval.
            if (DateTime.UtcNow >= _nextCheckpointTime)
            {
                Checkpoint(input.Checkpointer);
                _nextCheckpointTime = DateTime.UtcNow + CheckpointInterval;
            }
        }

        /// <summary>
        /// This method processes records, performing retries as needed.
        /// </summary>
        /// <param name="records">The records to be processed.</param>
        private void ProcessRecords(List<Record> records)
        {
            //Kiểm tra kết nối DB
            if (MysqlProvider.CheckHealth(configDB))
            {
                MysqlProvider.SyncBatchData(records, configDB, applicationName, maxErrorCountToShutDown);
                //Kiếm tra check lỗi quá số lần liên tiếp => Shutdown
                //int errorCount = 0;
                //foreach (Record rec in records)
                //{
                //    if (errorCount < maxErrorCountToShutDown)
                //    {
                //        MysqlProvider.SyncData(rec, configDB, applicationName, ref errorCount);
                //    }
                //    else
                //    {
                //        RecordProcessorEntity recordProcessorEntity = MysqlProvider.BuildRecordLogData(rec, applicationName, SyncDataErrorLevel.StopConsumer, $"{applicationName} Reach Max Error Count To ShutDown");
                //        MysqlProvider.SaveSyncDataError(recordProcessorEntity, configDB);
                //        throw new Exception($"{applicationName} Reach Max Error Count To ShutDown");
                //    }
                //}
            }
            else
            {
                //vào đây là worker stop
                Console.Error.WriteLine($"Connect to DB {configDB} fails");
                throw new Exception($"Connect to DB {configDB} fails");
            }
        }

        /// <summary>
        /// Check point bản ghi cuối cùng của gói dữ liệu
        /// </summary>
        /// <param name="checkpointer">The checkpointer used to do checkpoints.</param>
        private void Checkpoint(Checkpointer checkpointer)
        {
            Console.Error.WriteLine($"Checkpointing shard {_kinesisShardId}-Time: {DateTime.Now}" );
            checkpointer.Checkpoint(EinvoiceCheckpointErrorHandler.Create(NumRetries, Backoff,_kinesisShardId, configDB, applicationName));
            Console.Error.WriteLine($"End Checkpointing shard {_kinesisShardId}-Time: {DateTime.Now}");
        }

        public void LeaseLost(LeaseLossInput leaseLossInput)
        {
            //
            // Perform any necessary cleanup after losing your lease.  Checkpointing is not possible at this point.
            //
            Console.Error.WriteLine($"Lost lease on {_kinesisShardId}");
        }

        public void ShardEnded(ShardEndedInput shardEndedInput)
        {
            //
            // Once the shard has ended it means you have processed all records on the shard. To confirm completion the
            // KCL requires that you checkpoint one final time using the default checkpoint value.
            //
            Console.Error.WriteLine(
                $"All records for {_kinesisShardId} have been processed, starting final checkpoint");
            shardEndedInput.Checkpointer.Checkpoint();
        }

        public void ShutdownRequested(ShutdownRequestedInput shutdownRequestedInput)
        {
            Console.Error.WriteLine($"Shutdown has been requested for {_kinesisShardId}. Checkpointing");



            shutdownRequestedInput.Checkpointer.Checkpoint();
        }
    }

    class MainClass
    {
        /// <summary>
        /// This method creates a KclProcess and starts running an SampleRecordProcessor instance.
        /// </summary>
        public static void Main(string[] args)
        {
            try
            {
                IConfiguration Config = new ConfigurationBuilder()
                .AddJsonFile("appSettings.json", optional: false, reloadOnChange: true).Build();

                ConsumerConfig.mysqlDbConfig = Config.GetSection("MysqlDB").Value;
                ConsumerConfig.applicationName = Config.GetSection("ApplicationName").Value;
                ConsumerConfig.maxErrorCountToShutDown = int.Parse(Config.GetSection("MaxErrorCountToShutDown").Value);
                ConsumerConfig.logPlaintextData = bool.Parse(Config.GetSection("IsLogPlaintextData").Value);

                //string backoff = Config.GetSection("Backoff").Value;
                //string checkpointInterval = Config.GetSection("CheckpointInterval").Value;
                //string numRetries = Config.GetSection("NumRetries").Value;

                KclProcess.Create(new EinvoiceRecordProcessor()).Run();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
            }
        }
    }    
}
