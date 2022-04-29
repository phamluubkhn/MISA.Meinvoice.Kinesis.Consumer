using Amazon.Kinesis.ClientLibrary;
using System;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.Json;
using MySqlConnector;
using Dapper;
using System.Data;

namespace MISA.Meinvoice.Kinesis.Bank
{
    public class CustomerBankAccountRecordProcessor : IShardRecordProcessor
    {
        private static string configDB = Environment.GetEnvironmentVariable("DB_CONNECTION");


        /// <value>The time to wait before this record processor
        /// reattempts either a checkpoint, or the processing of a record.</value>
        private static readonly TimeSpan Backoff = TimeSpan.FromSeconds(3);

        /// <value>The interval this record processor waits between
        /// doing two successive checkpoints.</value>
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMinutes(1);

        /// <value>The maximum number of times this record processor retries either
        /// a failed checkpoint, or the processing of a record that previously failed.</value>
        private static readonly int NumRetries = 10;

        /// <value>The shard ID on which this record processor is working.</value>
        private string _kinesisShardId;

        /// <value>The next checkpoint time expressed in milliseconds.</value>
        private DateTime _nextCheckpointTime = DateTime.UtcNow;

        /// <summary>
        /// This method is invoked by the Amazon Kinesis Client Library before records from the specified shard
        /// are delivered to this SampleRecordProcessor.
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
            // Process records and perform all exception handling.
            ProcessRecordsWithRetries(input.Records);

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
        private void ProcessRecordsWithRetries(List<Record> records)
        {
            foreach (Record rec in records)
            {
                bool processedSuccessfully = false;
                string data = null;
                for (int i = 0; i < NumRetries; ++i)
                {
                    try
                    {
                        // is interpreted as UTF-8 characters.
                        string recordData = System.Text.Encoding.UTF8.GetString(rec.Data);
                        CustomerBankAccount Bank = JsonSerializer.Deserialize<CustomerBankAccount>(recordData);
                        SyncData(Bank);
                        // Uncomment the following if you wish to see the retrieved record data.
                        //object Bank = DeserializeFromStream(rec.Data);
                        //Console.Error.WriteLine(
                        //    String.Format("Retrieved record:\n\tpartition key = {0},\n\tsequence number = {1},\n\tdata = {2}",
                        //    rec.PartitionKey, rec.SequenceNumber, JsonSerializer.Serialize(Bank)));

                        Console.Error.WriteLine(
                            String.Format("Retrieved record:\n\tpartition key = {0},\n\tsequence number = {1},\n\tdata = {2}",
                            rec.PartitionKey, rec.SequenceNumber, data));

                        // Your own logic to process a record goes here.

                        processedSuccessfully = true;
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("Exception processing record data: " + data, e);
                        //Back off before retrying upon an exception.
                        Thread.Sleep(Backoff);
                    }
                }

                if (!processedSuccessfully)
                {
                    Console.Error.WriteLine("Couldn't process record " + rec + ". Skipping the record.");
                }
            }
        }

        /// <summary>
        /// This checkpoints the specified checkpointer with retries.
        /// </summary>
        /// <param name="checkpointer">The checkpointer used to do checkpoints.</param>
        private void Checkpoint(Checkpointer checkpointer)
        {
            Console.Error.WriteLine("Checkpointing shard " + _kinesisShardId);

            // You can optionally provide an error handling delegate to be invoked when checkpointing fails.
            // The library comes with a default implementation that retries for a number of times with a fixed
            // delay between each attempt. If you do not provide an error handler, the checkpointing operation
            // will not be retried, but processing will continue.
            checkpointer.Checkpoint(RetryingCheckpointErrorHandler.Create(NumRetries, Backoff));
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

        private void SyncData(CustomerBankAccount bank)
        {
            if (string.IsNullOrEmpty(configDB))
            {
                configDB = "server=3.1.42.67;database=tcb_meinvoice;port=3306;user=root;password=12345678@";
                //configDB = "server=172.4.1.241;database=tcb_meinvoice;port=3306;user=root;password=12345678@";
            }
            if (bank != null)
            {
                using (var _connection = new MySqlConnection(configDB))
                {
                    Console.WriteLine("Log: _connection.ConnectionString: " + _connection.ConnectionString);

                    if (_connection.State == ConnectionState.Closed)
                        _connection.Open();

                    Console.WriteLine("Log: State: " + _connection.State.ToString());
                    Console.WriteLine("Log: DB ServerVersion: " + _connection.ServerVersion);

                    try
                    {
                        DynamicParameters dynamicParameters = new DynamicParameters();
                        dynamicParameters.Add("BankAccountId", bank.bank_account_id);
                        dynamicParameters.Add("AcctCloseDate", bank.acct_close_date);
                        dynamicParameters.Add("Category", bank.category);
                        dynamicParameters.Add("CustomerId", bank.customer_id);
                        dynamicParameters.Add("AccNumber", bank.acc_number);
                        dynamicParameters.Add("Currency", bank.currency);
                        dynamicParameters.Add("Status", bank.status);
                        _connection.Execute("Proc_SyncCustomerBankAccount", dynamicParameters, commandType: CommandType.StoredProcedure);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception: " + ex.Message);
                        //TODO: Lưu log hoặc đẩy vào queue log
                        throw;
                    }
                    finally
                    {
                        if (_connection != null && _connection.State != ConnectionState.Closed)
                        {
                            _connection.Close();
                        }
                        _connection.Dispose();
                    }
                }
            }
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
                KclProcess.Create(new CustomerBankAccountRecordProcessor()).Run();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("ERROR: " + e);
            }
        }
    }

    public class CustomerBankAccount
    {
        /// <summary>
        /// id
        /// </summary>
        public Guid bank_account_id { get; set; }
        /// <summary>
        ///
        /// </summary>
        public DateTime acct_close_date { get; set; }
        /// <summary>
        /// Mã sản phẩm
        /// </summary>
        public string category { get; set; }
        /// <summary>
        /// id của khách hàng
        /// </summary>
        public string customer_id { get; set; }
        /// <summary>
        /// Số tài khoản
        /// </summary>
        public string acc_number { get; set; }
        /// <summary>
        /// Mã tiền tệ
        /// </summary>
        public string currency { get; set; }
        /// <summary>
        /// Trạng thái
        /// </summary>
        public int status { get; set; }

    }
}
