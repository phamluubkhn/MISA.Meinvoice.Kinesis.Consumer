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
using Amazon.Runtime;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis
{
    public class EinvoiceRecordProcessor : IShardRecordProcessor
    {
        private static string configDB = ConsumerConfig.mysqlDbConfig;

        private static int delayRetryTimeMilisecond = ConsumerConfig.delayRetryTimeMilisecond;

        private static string applicationName = ConsumerConfig.applicationName;

        private static int maxErrorCountToShutDown = ConsumerConfig.maxErrorCountToShutDown;

        private static int totalRecord = 0;

        private static ManualCheckpointer manualCheckpointer = new ManualCheckpointer()
        {
            Checkpointer = null,
            IsChecked = true
        };

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
            //ManualCheckpoint();
        }

        /// <summary>
        /// This method processes the given records and checkpoints using the given checkpointer.
        /// </summary>
        /// <param name="input">
        /// ProcessRecordsInput that contains records, a Checkpointer and contextual information.
        /// </param>
        public void ProcessRecords(ProcessRecordsInput input)
        {
            Console.Error.WriteLine("ProcessRecords Start v5 - Record count " + input.Records.Count);
            totalRecord += input.Records.Count;
            Console.Error.WriteLine("ProcessRecords Start v5 time" + DateTime.Now);
            Console.Error.WriteLine("NextCheckpointTime time" + _nextCheckpointTime);
            string errorCode = "";
            int positionError = -1;
            // Process records and perform all exception handling.
            ProcessRecords(input.Records, ref errorCode, ref positionError);
            Console.Error.WriteLine("ProcessRecords End v6 time" + DateTime.Now);
            // Checkpoint once every checkpoint interval.
            if (DateTime.UtcNow >= _nextCheckpointTime)
            {
                Checkpoint(input.Checkpointer);
                _nextCheckpointTime = DateTime.UtcNow + CheckpointInterval;
                //manualCheckpointer.IsChecked = true;
            }
            //else
            //{
            //    manualCheckpointer.Checkpointer = input.Checkpointer;
            //    manualCheckpointer.IsChecked = false;
            //}
            if (!string.IsNullOrEmpty(errorCode))
            {
                Record record = input.Records[positionError];
                CheckpointRecord(input.Checkpointer, record);
                throw new Exception($"{applicationName} Reach Max Error Count To ShutDown");
            }
        }

        public void ManualCheckpoint()
        {
            while (true)
            {
                if (!manualCheckpointer.IsChecked)
                {
                    lock (manualCheckpointer)
                    {
                        Checkpoint(manualCheckpointer.Checkpointer);
                        manualCheckpointer.IsChecked = true;
                    }
                }
                Thread.Sleep(TimeSpan.FromSeconds(60));
            }
        }

        /// <summary>
        /// This method processes records, performing retries as needed.
        /// </summary>
        /// <param name="records">The records to be processed.</param>
        private void ProcessRecords(List<Record> records, ref string errorCode, ref int positionError)
        {
            bool isBatchInsertSuccess = true;
            //Thằng này chỉ insert batch
            if (applicationName == KCLApplication.Transaction)
            {
                bool isApplyRetry = true;
                int retryNumber = 0;
                while (isApplyRetry)
                {
                    isApplyRetry = MysqlProvider.SyncBatchTransactionData(records, configDB, retryNumber);
                    if (isApplyRetry)
                    {
                        retryNumber++;
                        Console.Error.WriteLine($"Execute SyncBatchTransactionData Timeout :Retry number {retryNumber}");
                        Thread.Sleep(delayRetryTimeMilisecond);
                    }
                }
            }
            else if (applicationName == KCLApplication.Pl01GTGT)
            {
                isBatchInsertSuccess = MysqlProvider.SyncBatchPlgtgtData(records, configDB);
            }
            else if (applicationName == KCLApplication.CustomerBankAccount)
            {
                isBatchInsertSuccess = MysqlProvider.SyncBatchBankData(records, configDB);
            }
            if (!isBatchInsertSuccess || (applicationName == KCLApplication.Company || applicationName == KCLApplication.Customer || applicationName == KCLApplication.Currency))
            {
                MysqlProvider.SyncData(records, configDB, applicationName, maxErrorCountToShutDown, this._kinesisShardId, ref errorCode, ref positionError);
            }
        }

        /// <summary>
        /// Check point bản ghi cuối cùng của gói dữ liệu
        /// </summary>
        /// <param name="checkpointer">The checkpointer used to do checkpoints.</param>
        private void Checkpoint(Checkpointer checkpointer)
        {
            Console.Error.WriteLine($"Checkpointing shard {_kinesisShardId}-Time: {DateTime.Now}" );
            //checkpointer.Checkpoint(EinvoiceCheckpointErrorHandler.Create(NumRetries, Backoff,_kinesisShardId, configDB, applicationName));
            checkpointer.Checkpoint(RetryingCheckpointErrorHandler.Create(NumRetries, Backoff));
            Console.Error.WriteLine($"End Checkpointing shard {_kinesisShardId}-Time: {DateTime.Now}");
        }

        private void CheckpointRecord(Checkpointer checkpointer, Record record)
        {
            Console.Error.WriteLine($"CheckpointRecord shard {_kinesisShardId}-Time: {DateTime.Now}");
            checkpointer.Checkpoint(record);
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

                ConsumerConfig.applicationName = Config.GetSection("ApplicationName").Value;
                ConsumerConfig.maxErrorCountToShutDown = int.Parse(Config.GetSection("MaxErrorCountToShutDown").Value);
                ConsumerConfig.logPlaintextData = bool.Parse(Config.GetSection("IsLogPlaintextData").Value);
                ConsumerConfig.useSecretsManager = bool.Parse(Config.GetSection("UseSecretsManager").Value);
                if (Config.GetSection("DelayRetryTimeMilisecond") != null)
                {
                    ConsumerConfig.delayRetryTimeMilisecond = int.Parse(Config.GetSection("DelayRetryTimeMilisecond").Value);
                }
                if (ConsumerConfig.useSecretsManager)
                {
                    string rawConfig = null;
                    Console.Error.WriteLine($"Config ApiUrl: {Config.GetSection("ApiUrl").Value}");
                    Console.Error.WriteLine($"Config MysqlDB: {Config.GetSection("MysqlDB").Value}");
                    while (string.IsNullOrEmpty(rawConfig))
                    {
                        rawConfig = CommonFunction.GetMysqlConnectionString(Config.GetSection("ApiUrl").Value, Config.GetSection("MysqlDB").Value);
                        if (string.IsNullOrEmpty(rawConfig))
                        {
                            Thread.Sleep(ConsumerConfig.delayRetryTimeMilisecond);
                        }
                    }

                    ConsumerConfig.mysqlDbConfig = rawConfig;
                    Console.Error.WriteLine($"mysqlDbConfig result {rawConfig}");
                    bool pingStatus = MysqlProvider.CheckHealth(ConsumerConfig.mysqlDbConfig);
                    Console.Error.WriteLine($"Check SecretsManager result {rawConfig}-Ping status: {pingStatus}");
                }
                else
                {
                    ConsumerConfig.mysqlDbConfig = Config.GetSection("MysqlDB").Value;
                }
                KclProcess.Create(new EinvoiceRecordProcessor()).Run();



            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
            }
        }
    }    
}
