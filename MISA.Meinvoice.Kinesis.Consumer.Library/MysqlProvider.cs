using Amazon.DynamoDBv2;
using Amazon.Kinesis.ClientLibrary;
using Dapper;
using MySqlConnector;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class MysqlProvider
    {
        public static bool CheckHealth(string connectionString)
        {
            bool result = false;
            Console.Error.WriteLine("CheckHealth mysql start");
            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    if (connection.State == ConnectionState.Closed)
                        connection.Open();
                    result = connection.Ping();
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("CheckHealth mysql ex" + ex.Message);
                    return false;
                }
                finally
                {
                    connection.Dispose();
                    connection.Close();
                }
            }
            
            return result;
        }

        public static void SyncData(Record rec, string configDB, string application, ref int errorCount)
        {
            try
            {
                bool isError = false;
                DynamicParameters dynamicParameters = new DynamicParameters();
                string procedureName = BuildSyncDataParam(rec.Data, application, dynamicParameters);
                using (var _connection = new MySqlConnection(configDB))
                {

                    if (_connection.State == ConnectionState.Closed)
                        _connection.Open();

                    try
                    {
                        if (!string.IsNullOrEmpty(procedureName))
                        {
                            _connection.Execute(procedureName, dynamicParameters, commandType: CommandType.StoredProcedure);
                        }
                    }
                    catch (MySqlException ex)
                    {
                        //Switch từ sqlstate
                        //if (ex != null && !string.IsNullOrEmpty(ex.SqlState))
                        //{
                        //    Console.WriteLine("Exception: Message-" + ex.Message);
                        //    Console.WriteLine("Exception: SqlState-" + ex.SqlState);
                        //    SyncErrorExtraData syncErrorExtraData = new SyncErrorExtraData();
                        //    switch (ex.SqlState)
                        //    {
                        //        case MysqlState.customer_status_empty:
                        //        case MysqlState.customer_status_invalid:
                        //            syncErrorExtraData.SqlState = ex.SqlState;
                        //            syncErrorExtraData.SqlStateMessage = ex.Message;
                        //            break;
                        //        default:
                        //            break;
                        //    }
                        //    RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec, KCLApplication.Customer, SyncDataErrorLevel.MysqlBusiness, ex.Message, syncErrorExtraData);
                        //    SaveSyncDataError(recordProcessorEntity, configDB);
                        //}
                        isError = true;
                        SyncErrorExtraData syncErrorExtraData = new SyncErrorExtraData();
                        if (ex != null && !string.IsNullOrEmpty(ex.SqlState))
                        {
                            syncErrorExtraData.SqlState = ex.SqlState;
                            syncErrorExtraData.SqlStateMessage = ex.Message;
                        }
                        syncErrorExtraData.SqlState = ex.SqlState;
                        syncErrorExtraData.SqlStateMessage = ex.Message;
                        RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec, application, SyncDataErrorLevel.MysqlException, ex.Message, syncErrorExtraData);
                        SaveSyncDataError(recordProcessorEntity, configDB);
                    }
                    catch (Exception ex)
                    {
                        isError = true;
                        Console.WriteLine("Exception: " + ex.Message);
                        RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec, application, SyncDataErrorLevel.OtherException, ex.Message);
                        SaveSyncDataError(recordProcessorEntity, configDB);
                    }
                    finally
                    {
                        if (isError)
                        {
                            errorCount += 1;
                        }
                        else
                        {
                            errorCount = 0;
                        }
                        if (_connection != null && _connection.State != ConnectionState.Closed)
                        {
                            _connection.Close();
                        }
                        _connection.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec, application, SyncDataErrorLevel.RecordException, ex.Message);
                SaveSyncDataError(recordProcessorEntity, configDB);
            }
        }

        public static void SyncData(List<Record> rec, string configDB, string application, int maxErrorCountToShutDown, string kinesisShardId, ref string errorCode, ref int positionError)
        {
            using (var _connection = new MySqlConnection(configDB))
            {
                try
                {
                    if (_connection.State == ConnectionState.Closed)
                        _connection.Open();

                    int errorCount = 0;
                    foreach (var item in rec)
                    {
                        positionError++;
                        if (errorCount >= maxErrorCountToShutDown)
                        {
                            RecordProcessorEntity recordProcessorEntity = MysqlProvider.BuildRecordLogData(item, application, SyncDataErrorLevel.StopConsumer, $"{application} Reach Max Error Count To ShutDown");
                            SaveSyncDataError(recordProcessorEntity, configDB);
                            errorCode = "E1000";
                            break;
                            //throw new Exception($"{application} Reach Max Error Count To ShutDown");
                        }
                        bool isError = false;
                        try
                        {
                            //Console.WriteLine($"Execute Start {DateTime.Now.Second}- {DateTime.Now.Millisecond} for {item.SequenceNumber} " );
                            DynamicParameters dynamicParameters = new DynamicParameters();
                            string procedureName = BuildSyncDataParam(item.Data, application, dynamicParameters, item.SequenceNumber);
                            if (!string.IsNullOrEmpty(procedureName))
                            {
                                _connection.Execute(procedureName, dynamicParameters, commandType: CommandType.StoredProcedure);
                            }
                            //Console.WriteLine($"Execute End {DateTime.Now.Second}- {DateTime.Now.Millisecond} for {item.SequenceNumber} ");
                        }
                        catch (MySqlException ex)
                        {
                            isError = true;
                            SyncErrorExtraData syncErrorExtraData = new SyncErrorExtraData();
                            if (ex != null && !string.IsNullOrEmpty(ex.SqlState))
                            {
                                syncErrorExtraData.SqlState = ex.SqlState;
                                syncErrorExtraData.SqlStateMessage = ex.Message;
                            }
                            syncErrorExtraData.SqlState = ex.SqlState;
                            syncErrorExtraData.SqlStateMessage = ex.Message;
                            RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(item, application, SyncDataErrorLevel.MysqlException, ex.Message, syncErrorExtraData);
                            SaveSyncDataError(recordProcessorEntity, configDB);
                        }
                        catch (Exception e)
                        {
                            isError = true;
                            Console.WriteLine("Exception: " + e.Message);
                            RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(item, application, SyncDataErrorLevel.RecordException, e.Message);
                            SaveSyncDataError(recordProcessorEntity, configDB);
                        }
                        finally
                        {
                            if (isError)
                            {
                                errorCount += 1;
                            }
                            else
                            {
                                errorCount = 0;
                            }
                        }

                    }
                }
                catch (Exception ex)
                {
                    if (_connection != null && _connection.State != ConnectionState.Closed)
                    {
                        _connection.Close();
                    }
                    _connection.Dispose();

                    throw;
                }
            }

        }

        private static string ProcessTransactionRecord(Record record, string configDB, string application, ref List<string> rowsCheckDuplicate)
        {
            string result = "";
            try
            {
                string recordData = System.Text.Encoding.UTF8.GetString(record.Data);
                TRANSACTION_DATA transaction = JsonConvert.DeserializeObject<TRANSACTION_DATA>(recordData);
                result = string.Format("(UUID(),{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32},{33}, now())",
                    transaction.ENTRY_ID == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.ENTRY_ID)}'",
                    transaction.ENTRY_TYPE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.ENTRY_TYPE)}'",
                    transaction.BUYER_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.BUYER_CODE)}'",
                    transaction.BRANCH_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.BRANCH_CODE)}'",
                    transaction.BUYER_BANK_ACCOUNT == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.BUYER_BANK_ACCOUNT)}'",
                    transaction.CURRENCY_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.CURRENCY_CODE)}'",
                    transaction.PAYMENT_METHOD_NAME == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.PAYMENT_METHOD_NAME)}'",
                    transaction.INV_TYPE_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.INV_TYPE_CODE)}'",
                    transaction.INV_NOTE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.INV_NOTE)}'",
                    transaction.TRANSFER_DATE.HasValue ? $"'{transaction.TRANSFER_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL",
                    transaction.TRANS_NO == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.TRANS_NO)}'",

                    transaction.EXCHANGE_RATE.HasValue ? transaction.EXCHANGE_RATE.ToString() : "NULL",
                    transaction.ITEM_NAME == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.ITEM_NAME)}'",
                    transaction.UNIT_NAME == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.UNIT_NAME)}'",
                    transaction.QUANTITY.HasValue ? transaction.QUANTITY.ToString() : "NULL",
                    transaction.UNIT_PRICE.HasValue ? transaction.UNIT_PRICE.ToString() : "NULL",
                    transaction.VAT_CATEGORY_PERCENTAGE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.VAT_CATEGORY_PERCENTAGE)}'",
                    transaction.VAT_AMOUNT.HasValue ? transaction.VAT_AMOUNT.ToString() : "NULL",
                    transaction.TOTAL_AMOUNT_WITHOUT_VAT.HasValue ? transaction.TOTAL_AMOUNT_WITHOUT_VAT.ToString() : "NULL",
                    transaction.TOTAL_AMOUNT.HasValue ? transaction.TOTAL_AMOUNT.ToString() : "NULL",
                    transaction.IS_SOURCE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.IS_SOURCE)}'",

                    transaction.MODULE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.MODULE)}'",
                    transaction.PROCESS_DATE.HasValue ? $"'{transaction.PROCESS_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL",
                    transaction.CREATION_DATE.HasValue ? $"'{transaction.CREATION_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL",
                    transaction.ACCOUNT_CO_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.ACCOUNT_CO_CODE)}'",
                    transaction.PRIORITY.HasValue ? transaction.PRIORITY.ToString() : "NULL",
                    transaction.PALCAT == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.PALCAT)}'",
                    transaction.AMOUNT_LCY.HasValue ? transaction.AMOUNT_LCY.ToString() : "NULL",
                    transaction.PRODCAT.HasValue ? transaction.PRODCAT.ToString() : "NULL",
                    transaction.TRANSACTION_TYPE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.TRANSACTION_TYPE)}'",
                    transaction.REVERT_FLAG == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.REVERT_FLAG)}'",
                    transaction.TRANSACTION_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.TRANSACTION_CODE)}'",
                    transaction.ORIGIN_TRANS_REF == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.ORIGIN_TRANS_REF)}'",
                    MySqlHelper.EscapeString(record.SequenceNumber)); ;

                string rowCheckDuplicate = $"('{MySqlHelper.EscapeString(record.SequenceNumber)}', '{transaction.PROCESS_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}')";
                rowsCheckDuplicate.Add(rowCheckDuplicate);

            }
            catch (Exception e)
            {

                Console.WriteLine("Exception: " + e.Message);
                result = "";
                RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(record, application, SyncDataErrorLevel.RecordException, e.Message);
                SaveSyncDataError(recordProcessorEntity, configDB);
            }
            return result;
        }

        public static bool SyncBatchTransactionData(List<Record> rec, string configDB)
        {
            bool result = true;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("INSERT INTO trans_data VALUES ");

            StringBuilder stringBuilderTableCheckDuplicate = new StringBuilder();
            stringBuilderTableCheckDuplicate.Append("INSERT INTO trans_data_check_duplicate VALUES ");

            using (MySqlConnection mConnection = new MySqlConnection(configDB))
            {
                List<string> rows = new List<string>();
                List<string> rowsCheckDuplicate = new List<string>();
                foreach (var item in rec)
                {
                    string rowText = ProcessTransactionRecord(item, configDB, KCLApplication.Transaction, ref rowsCheckDuplicate);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        rows.Add(rowText);
                    }
                }
                stringBuilder.Append(string.Join(",", rows));
                stringBuilder.Append(";");

                stringBuilderTableCheckDuplicate.Append(string.Join(",", rowsCheckDuplicate));
                stringBuilderTableCheckDuplicate.Append("as a ON DUPLICATE KEY UPDATE PROCESS_DATE = a.PROCESS_DATE");
                stringBuilderTableCheckDuplicate.Append(";");


                string cmdExecuteTransdata = stringBuilder.ToString();
                string cmdExecute = stringBuilder.ToString();
                string cmdExecuteCheckDupliate = stringBuilderTableCheckDuplicate.ToString();
                mConnection.Open();
                using MySqlTransaction transaction = mConnection.BeginTransaction();
                try
                {
                    mConnection.Execute(cmdExecuteTransdata, transaction: transaction, commandType: CommandType.Text);
                    string cmdExecuteTransdataOrg = cmdExecuteTransdata.Replace("trans_data", "trans_data_org");
                    mConnection.Execute(cmdExecuteTransdataOrg, transaction: transaction, commandType: CommandType.Text);

                    mConnection.Execute(cmdExecuteCheckDupliate, transaction: transaction, commandType: CommandType.Text);
                    //mConnection.Execute("Proc_SyncTransactionTempData", null, commandType: CommandType.StoredProcedure);
                    transaction.Commit();
                }
                catch (Exception e)
                {
                    RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec[0], KCLApplication.Transaction, SyncDataErrorLevel.BatchInsertException, e.Message);
                    SaveSyncDataError(recordProcessorEntity, configDB);
                    transaction.Rollback();
                    result = false;
                }
            }
            return result;
        }

        private static string ProcessPlgtgtRecord(Record record, string configDB, string application)
        {
            string result = "";
            try
            {
                string recordData = System.Text.Encoding.UTF8.GetString(record.Data);
                Pl01gtgt pl01Gtgt = JsonConvert.DeserializeObject<Pl01gtgt>(recordData);
                bool reversalMarker = true;
                if (string.IsNullOrEmpty(pl01Gtgt.REVERSAL_MARKER))
                {
                    reversalMarker = false;
                }
                result = string.Format("({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18}, now())",
                    pl01Gtgt.contract_number == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.contract_number)}'",
                    pl01Gtgt.COMPANY == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.COMPANY)}'",
                    pl01Gtgt.pl_category,
                    pl01Gtgt.booking_date.HasValue ? $"'{pl01Gtgt.booking_date.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL",
                    pl01Gtgt.amount,
                    pl01Gtgt.description == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.description)}'",
                    pl01Gtgt.TYPE_CODE.HasValue ? pl01Gtgt.TYPE_CODE.ToString() : "NULL",
                    pl01Gtgt.PURPOSE.HasValue ? pl01Gtgt.PURPOSE.ToString() : "NULL",
                    pl01Gtgt.product_category.HasValue ? pl01Gtgt.product_category.ToString() : "NULL",
                    pl01Gtgt.currency == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.currency)}'",
                    pl01Gtgt.amount_foreign_currency.HasValue ? pl01Gtgt.amount_foreign_currency.ToString() : "NULL",
                    pl01Gtgt.TRANSACTION_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.TRANSACTION_CODE)}'",
                    pl01Gtgt.SYSTEM_ID == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.SYSTEM_ID)}'",
                    pl01Gtgt.customer_code == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.customer_code)}'",
                    pl01Gtgt.SOURCE_ID == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.SOURCE_ID)}'",
                    pl01Gtgt.VALUE_DATE.HasValue ? $"'{pl01Gtgt.VALUE_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL",
                    reversalMarker ? "1" : "0",
                    pl01Gtgt.COMPANY_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.COMPANY_CODE)}'",
                    MySqlHelper.EscapeString(record.SequenceNumber)
                    );         
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message);
                result = "";
                RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(record, application, SyncDataErrorLevel.RecordException, e.Message);
                SaveSyncDataError(recordProcessorEntity, configDB);
            }
            return result;
        }

        public static bool SyncBatchPlgtgtData(List<Record> rec, string configDB)
        {
            bool result = true;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("INSERT INTO pl01gtgt (TRANS_NO, COMPANY, PL_CATEGORY, BOOKING_DATE, AMOUNT, DESCRIPTION, TYPE_CODE, PURPOSE, PRODCAT, CURRENCY, AMOUNT_FCY, TRANSACTION_CODE, SYSTEM_ID, BUYER_CODE, SOURCE_ID, VALUE_DATE, REVERSAL_MARKER, COMPANY_CODE, SEQUENCE_NUMBER, MODIFY_DATE) VALUES ");
            using (MySqlConnection mConnection = new MySqlConnection(configDB))
            {
                List<string> rows = new List<string>();
                foreach (var item in rec)
                {
                    string rowText = ProcessPlgtgtRecord(item, configDB, KCLApplication.Transaction);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        rows.Add(rowText);
                    }
                }
                stringBuilder.Append(string.Join(",", rows));
                stringBuilder.Append("as a ON DUPLICATE KEY UPDATE TRANS_NO = a.TRANS_NO, COMPANY = a.COMPANY, PL_CATEGORY = a.PL_CATEGORY, BOOKING_DATE = a.BOOKING_DATE, AMOUNT = a.AMOUNT, DESCRIPTION = a.DESCRIPTION, TYPE_CODE = a.TYPE_CODE, PURPOSE = a.PURPOSE,PRODCAT = a.PRODCAT,CURRENCY = a.CURRENCY,AMOUNT_FCY= a.AMOUNT_FCY,TRANSACTION_CODE = a.TRANSACTION_CODE,SYSTEM_ID = a.SYSTEM_ID,BUYER_CODE = a.BUYER_CODE,VALUE_DATE = a.VALUE_DATE,REVERSAL_MARKER = a.REVERSAL_MARKER,COMPANY_CODE = a.COMPANY_CODE,SEQUENCE_NUMBER = a.SEQUENCE_NUMBER,MODIFY_DATE = NOW();");
                stringBuilder.Append(";");
                mConnection.Open();
                string cmdExecuteTemp = stringBuilder.ToString();
                string cmdExecute = stringBuilder.ToString();
                using MySqlTransaction transaction = mConnection.BeginTransaction();
                try
                {
                    mConnection.Execute(cmdExecute, transaction: transaction, commandType: CommandType.Text);
                    string cmdExecuteDataTemp = cmdExecuteTemp.Replace("pl01gtgt", "pl01gtgt_temp");
                    cmdExecuteDataTemp = cmdExecuteDataTemp.Replace("as a ON DUPLICATE KEY UPDATE TRANS_NO = a.TRANS_NO, COMPANY = a.COMPANY, PL_CATEGORY = a.PL_CATEGORY, BOOKING_DATE = a.BOOKING_DATE, AMOUNT = a.AMOUNT, DESCRIPTION = a.DESCRIPTION, TYPE_CODE = a.TYPE_CODE, PURPOSE = a.PURPOSE,PRODCAT = a.PRODCAT,CURRENCY = a.CURRENCY,AMOUNT_FCY= a.AMOUNT_FCY,TRANSACTION_CODE = a.TRANSACTION_CODE,SYSTEM_ID = a.SYSTEM_ID,BUYER_CODE = a.BUYER_CODE,VALUE_DATE = a.VALUE_DATE,REVERSAL_MARKER = a.REVERSAL_MARKER,COMPANY_CODE = a.COMPANY_CODE,SEQUENCE_NUMBER = a.SEQUENCE_NUMBER,MODIFY_DATE = NOW();", ";");
                    mConnection.Execute(cmdExecuteDataTemp, transaction: transaction, commandType: CommandType.Text);
                    transaction.Commit();
                }
                catch (Exception e)
                {
                    RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec[0], KCLApplication.Pl01GTGT, SyncDataErrorLevel.BatchInsertException, e.Message);
                    SaveSyncDataError(recordProcessorEntity, configDB);
                    transaction.Rollback();
                    result = false;
                }
                finally
                {
                    mConnection.Dispose();
                    mConnection.Close();
                }
            }
            return result;
        }

        public static bool SyncBatchBankData(List<Record> rec, string configDB)
        {
            bool result = true;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("INSERT INTO customerbankaccount (ID, CustomerID, AccountNumber, CloseDate, Category, Currency, Status, ModifiedDate, DsPartitionDate) VALUES ");

            using (MySqlConnection mConnection = new MySqlConnection(configDB))
            {
                List<string> rows = new List<string>();
                foreach (var item in rec)
                {
                    string rowText = ProcessBankRecord(item, configDB, KCLApplication.Transaction);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        rows.Add(rowText);
                    }
                }
                stringBuilder.Append(string.Join(",", rows));
                stringBuilder.Append("as a ON DUPLICATE KEY UPDATE CustomerID = a.CustomerID, CloseDate = a.CloseDate, Category = a.Category, Currency = a.Currency, Status = a.Status, DsPartitionDate = a.DsPartitionDate, ModifiedDate  = NOW();");
                stringBuilder.Append(";");
                mConnection.Open();
                string cmdExecuteTemp = stringBuilder.ToString();
                string cmdExecute = stringBuilder.ToString();
                using MySqlTransaction transaction = mConnection.BeginTransaction();
                try
                {
                    mConnection.Execute(cmdExecute, transaction: transaction, commandType: CommandType.Text);
                    string cmdExecuteDataTemp = cmdExecuteTemp.Replace("customerbankaccount", "customerbankaccount_temp");
                    cmdExecuteDataTemp = cmdExecuteDataTemp.Replace("as a ON DUPLICATE KEY UPDATE CustomerID = a.CustomerID, CloseDate = a.CloseDate, Category = a.Category, Currency = a.Currency, Status = a.Status, DsPartitionDate = a.DsPartitionDate, ModifiedDate  = NOW();", ";");
                    mConnection.Execute(cmdExecuteDataTemp, transaction: transaction, commandType: CommandType.Text);
                    transaction.Commit();
                }
                catch (Exception e)
                {
                    RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(rec[0], KCLApplication.CustomerBankAccount, SyncDataErrorLevel.BatchInsertException, e.Message);
                    SaveSyncDataError(recordProcessorEntity, configDB);
                    transaction.Rollback();
                    result = false;
                }
                finally
                {
                    mConnection.Dispose();
                    mConnection.Close();
                }
            }
            return result;
        }



        private static string ProcessBankRecord(Record record, string configDB, string application)
        {
            string result = "";
            try
            {
                string recordData = System.Text.Encoding.UTF8.GetString(record.Data);
                CUSTOMER_BANK_ACCOUNT bank = JsonConvert.DeserializeObject<CUSTOMER_BANK_ACCOUNT>(recordData);
                result = string.Format("({0},{1},{2},{3},{4},{5},{6}, now(), {7})",
                    $"'{Guid.NewGuid()}'",
                    bank.CUSTOMER_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(bank.CUSTOMER_CODE)}'",
                    bank.ACCOUNT_NUMBER == null ? "NULL" : $"'{MySqlHelper.EscapeString(bank.ACCOUNT_NUMBER)}'",
                    bank.ACCOUNT_CLOSED_DATE.HasValue ? $"'{bank.ACCOUNT_CLOSED_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL",
                    bank.CATEGORY == null ? "NULL" : $"'{MySqlHelper.EscapeString(bank.CATEGORY)}'",
                    bank.CURRENCY == null ? "NULL" : $"'{MySqlHelper.EscapeString(bank.CURRENCY)}'",
                    bank.STATUS.ToString(),
                    bank.DS_PARTITION_DATE.HasValue ? $"'{bank.DS_PARTITION_DATE.Value.ToString("yyyy-MM-dd HH:mm:ss")}'" : "NULL"); 
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message);
                result = "";
                RecordProcessorEntity recordProcessorEntity = BuildRecordLogData(record, application, SyncDataErrorLevel.RecordException, e.Message);
                SaveSyncDataError(recordProcessorEntity, configDB);
            }
            return result;
        }

        public static void ReSyncData(string id, byte[] recordData, string configDB, string application)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            string procedureName = BuildSyncDataParam(recordData, application, dynamicParameters);
            if (!string.IsNullOrEmpty(procedureName))
            {
                dynamicParameters.Add("Id", id);
                procedureName = $"{procedureName}_Resync";
                using (var _connection = new MySqlConnection(configDB))
                {

                    if (_connection.State == ConnectionState.Closed)
                        _connection.Open();

                    try
                    {
                        if (!string.IsNullOrEmpty(procedureName))
                        {
                            _connection.Execute(procedureName, dynamicParameters, commandType: CommandType.StoredProcedure);
                        }
                    }
                    catch (MySqlException ex)
                    {
                        //TODO: Tính sau
                    }
                    catch (Exception ex)
                    {
                        //TODO: Tính sau
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

        private static string BuildSyncDataParam(byte[] data, string application, DynamicParameters dynamicParameters, string sequenceNumber = null)
        {
            string procedureName = "";
            try
            {
                string recordData = System.Text.Encoding.UTF8.GetString(data);
                if (ConsumerConfig.logPlaintextData)
                {
                    Console.WriteLine("Record data decoded: " + recordData);
                }
                //Chi tiết về Record
                //https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html
                //Amazon.DynamoDBv2.Model.Record kinesisStreamRecord = JsonConvert.DeserializeObject<Amazon.DynamoDBv2.Model.Record>(recordData, new KinesisDatetimeConverter());
                switch (application)
                {
                    case KCLApplication.Customer:
                        //STAGING_CUSTOMER customer = BuildConsumerObject<STAGING_CUSTOMER>(kinesisStreamRecord);
                        STAGING_CUSTOMER customer = JsonConvert.DeserializeObject<STAGING_CUSTOMER>(recordData);
                        if (customer != null)
                        {
                            dynamicParameters.Add("CustomerID", customer.CUSTOMER_ID);
                            dynamicParameters.Add("CustomerAddress", customer.ADDRESS);
                            dynamicParameters.Add("CustomerEmail", customer.EMAIL);
                            dynamicParameters.Add("CustomerName", customer.BUYER_LEGAL_NAME);
                            dynamicParameters.Add("CustomerTaxCode", customer.TAX_CODE);
                            dynamicParameters.Add("Priority", customer.PRIORITY);
                            dynamicParameters.Add("PartitionDate", customer.DS_PARTITION_DATE);
                            procedureName = "Proc_SyncCustomerData";
                        }
                        else
                        {
                            throw new Exception("Deserialize STAGING_CUSTOMER Fails");
                        }
                        break;
                    case KCLApplication.CustomerBankAccount:
                        //CUSTOMER_BANK_ACCOUNT customerBankAccount = BuildConsumerObject<CUSTOMER_BANK_ACCOUNT>(kinesisStreamRecord);
                        CUSTOMER_BANK_ACCOUNT customerBankAccount = JsonConvert.DeserializeObject<CUSTOMER_BANK_ACCOUNT>(recordData);
                        if (customerBankAccount != null)
                        {
                            dynamicParameters.Add("CustomerID", customerBankAccount.CUSTOMER_CODE);
                            dynamicParameters.Add("AccountNumber", customerBankAccount.ACCOUNT_NUMBER);
                            dynamicParameters.Add("CloseDate", customerBankAccount.ACCOUNT_CLOSED_DATE);
                            dynamicParameters.Add("Category", customerBankAccount.CATEGORY);
                            dynamicParameters.Add("Currency", customerBankAccount.CURRENCY);
                            dynamicParameters.Add("Status", customerBankAccount.STATUS);
                            dynamicParameters.Add("PartitionDate", customerBankAccount.DS_PARTITION_DATE);
                            procedureName = "Proc_SyncCustomerBankAccount";
                        }
                        else
                        {
                            throw new Exception("Deserialize CUSTOMER_BANK_ACCOUNT Fails");
                        }
                        break;
                    case KCLApplication.Company:
                        //COMPANY company = BuildConsumerObject<COMPANY>(kinesisStreamRecord);
                        COMPANY company = JsonConvert.DeserializeObject<COMPANY>(recordData);
                        if (company != null)
                        {
                            dynamicParameters.Add("CompanyCode", company.COMPANY_CODE);
                            dynamicParameters.Add("CompanyName", company.NAME);
                            dynamicParameters.Add("Status", company.STATUS);
                            dynamicParameters.Add("PartitionDate", company.DS_PARTITION_DATE);
                            procedureName = "Proc_SyncCompanyData";
                        }
                        else
                        {
                            throw new Exception("Deserialize COMPANY Fails");
                        }
                        break;
                    case KCLApplication.Transaction:
                        //TRANSACTION_DATA transaction = BuildConsumerObject<TRANSACTION_DATA>(kinesisStreamRecord);
                        TRANSACTION_DATA transaction = JsonConvert.DeserializeObject<TRANSACTION_DATA>(recordData);
                        if (transaction != null)
                        {
                            dynamicParameters.Add("ENTRY_ID", transaction.ENTRY_ID);
                            dynamicParameters.Add("ENTRY_TYPE", transaction.ENTRY_TYPE);
                            dynamicParameters.Add("BUYER_CODE", transaction.BUYER_CODE);
                            dynamicParameters.Add("BRANCH_CODE", transaction.BRANCH_CODE);
                            dynamicParameters.Add("BUYER_BANK_ACCOUNT", transaction.BUYER_BANK_ACCOUNT);
                            dynamicParameters.Add("CURRENCY_CODE", transaction.CURRENCY_CODE);
                            dynamicParameters.Add("PAYMENT_METHOD_NAME", transaction.PAYMENT_METHOD_NAME);
                            dynamicParameters.Add("INV_TYPE_CODE", transaction.INV_TYPE_CODE);
                            dynamicParameters.Add("INV_NOTE", transaction.INV_NOTE);
                            dynamicParameters.Add("TRANFER_DATE", transaction.TRANSFER_DATE);
                            dynamicParameters.Add("TRANS_NO", transaction.TRANS_NO);
                            dynamicParameters.Add("EXCHANGE_RATE", transaction.EXCHANGE_RATE);
                            dynamicParameters.Add("ITEM_NAME", transaction.ITEM_NAME);
                            dynamicParameters.Add("UNIT_NAME", transaction.UNIT_NAME);
                            dynamicParameters.Add("QUANTITY", transaction.QUANTITY);
                            dynamicParameters.Add("UNIT_PRICE", transaction.UNIT_PRICE);
                            dynamicParameters.Add("VAT_CATEGORY_PERCENTAGE", transaction.VAT_CATEGORY_PERCENTAGE);
                            dynamicParameters.Add("VAT_AMOUNT", transaction.VAT_AMOUNT);
                            dynamicParameters.Add("TOTAL_AMOUNT_WITHOUT_VAT", transaction.TOTAL_AMOUNT_WITHOUT_VAT);
                            dynamicParameters.Add("TOTAL_AMOUNT", transaction.TOTAL_AMOUNT);
                            dynamicParameters.Add("IS_SOURCE", transaction.IS_SOURCE);
                            dynamicParameters.Add("MODULE", transaction.MODULE);
                            dynamicParameters.Add("PROCESS_DATE", transaction.PROCESS_DATE);
                            dynamicParameters.Add("CREATION_DATE", transaction.CREATION_DATE);
                            dynamicParameters.Add("ACCOUNT_CO_CODE", transaction.ACCOUNT_CO_CODE);
                            dynamicParameters.Add("PRIORITY", transaction.PRIORITY);
                            dynamicParameters.Add("PALCAT", transaction.PALCAT);
                            dynamicParameters.Add("AMOUNT_LCY", transaction.AMOUNT_LCY);
                            dynamicParameters.Add("PRODCAT", transaction.PRODCAT);
                            dynamicParameters.Add("TRANSACTION_TYPE", transaction.TRANSACTION_TYPE);
                            dynamicParameters.Add("REVERT_FLAG", transaction.REVERT_FLAG);
                            dynamicParameters.Add("TRANSACTION_CODE", transaction.TRANSACTION_CODE);
                            dynamicParameters.Add("ORIGIN_TRANS_REF", transaction.ORIGIN_TRANS_REF);
                            dynamicParameters.Add("SequenceNumber", sequenceNumber);

                            procedureName = "Proc_SyncTransactionData";
                        }
                        else
                        {
                            throw new Exception("Deserialize TRANSACTION_DATA Fails");
                        }
                        break;
                    case KCLApplication.Pl01GTGT:
                        Pl01gtgt pl01Gtgt = JsonConvert.DeserializeObject<Pl01gtgt>(recordData);
                        if (pl01Gtgt != null)
                        {
                            bool reversalMarker = true;
                            if (string.IsNullOrEmpty(pl01Gtgt.REVERSAL_MARKER))
                            {
                                reversalMarker = false;
                            }

                            dynamicParameters.Add("TRANS_NO", pl01Gtgt.contract_number);
                            dynamicParameters.Add("COMPANY", pl01Gtgt.COMPANY);
                            dynamicParameters.Add("PL_CATEGORY", pl01Gtgt.pl_category);
                            dynamicParameters.Add("BOOKING_DATE", pl01Gtgt.booking_date);
                            dynamicParameters.Add("AMOUNT", pl01Gtgt.amount);
                            dynamicParameters.Add("DESCRIPTION", pl01Gtgt.description);
                            dynamicParameters.Add("TYPE_CODE", pl01Gtgt.TYPE_CODE);
                            dynamicParameters.Add("PURPOSE", pl01Gtgt.PURPOSE);
                            dynamicParameters.Add("PRODCAT", pl01Gtgt.product_category);
                            dynamicParameters.Add("CURRENCY", pl01Gtgt.currency);
                            dynamicParameters.Add("AMOUNT_FCY", pl01Gtgt.amount_foreign_currency);
                            dynamicParameters.Add("TRANSACTION_CODE", pl01Gtgt.TRANSACTION_CODE);
                            dynamicParameters.Add("SYSTEM_ID", pl01Gtgt.SYSTEM_ID);
                            dynamicParameters.Add("BUYER_CODE", pl01Gtgt.customer_code);
                            dynamicParameters.Add("SOURCE_ID", pl01Gtgt.SOURCE_ID);
                            dynamicParameters.Add("VALUE_DATE", pl01Gtgt.VALUE_DATE);
                            dynamicParameters.Add("REVERSAL_MARKER", reversalMarker);
                            dynamicParameters.Add("COMPANY_CODE", pl01Gtgt.COMPANY_CODE);
                            dynamicParameters.Add("SequenceNumber", sequenceNumber);

                            procedureName = "Proc_SyncPl01GtgtData";
                        }
                        else
                        {
                            throw new Exception("Deserialize pl01Gtgt Fails");
                        }
                        break;
                    default:
                        break;
                }
            }
            catch (Exception)
            {
                throw;
            }
            return procedureName;
        }

        public static List<RecordSyncError> GetListRecordSyncError(string configDB)
        {
            List<RecordSyncError> recordSyncErrors = new List<RecordSyncError>();
            try
            {
                using (var _connection = new MySqlConnection(configDB))
                {

                    if (_connection.State == ConnectionState.Closed)
                        _connection.Open();

                    recordSyncErrors = _connection.Query<RecordSyncError>("Proc_GetListRecordSyncError", null, commandType: CommandType.StoredProcedure).AsList();
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Exececute proc  GetListRecordSyncError {ex.Message}");
                //TODO: Log lỗi
            }
            return recordSyncErrors;
        }


        public static RecordProcessorEntity BuildRecordLogData(Record rec, string consumer, int errorLevel, string errorDetail = null, SyncErrorExtraData syncErrorExtraData = null)
        {
            RecordProcessorEntity data = new RecordProcessorEntity()
            {
                Record = rec,
                Consumer = consumer,
                ErrorLevel = errorLevel,
                ErrorDetail = errorDetail,
                ErrorTime = DateTime.Now
            };
            if (syncErrorExtraData != null)
            {
                data.ExtraData = syncErrorExtraData;
            }
            return data;
        }

        public static void SaveSyncDataError(RecordProcessorEntity recError, string configDB)
        {
            using (var _connection = new MySqlConnection(configDB))
            {

                if (_connection.State == ConnectionState.Closed)
                    _connection.Open();

                try
                {
                    DynamicParameters dynamicParameters = new DynamicParameters();
                    dynamicParameters.Add("Consumer", recError.Consumer);
                    dynamicParameters.Add("RecordData", recError.Record.Data);
                    dynamicParameters.Add("SequenceNumber", recError.Record.SequenceNumber);
                    dynamicParameters.Add("SubSequenceNumber", recError.Record.SubSequenceNumber);
                    dynamicParameters.Add("PartitionKey", recError.Record.PartitionKey);
                    dynamicParameters.Add("ApproximateArrivalTimestamp", recError.Record.ApproximateArrivalTimestamp);
                    dynamicParameters.Add("ErrorDetail", recError.ErrorDetail);
                    dynamicParameters.Add("ErrorLevel", recError.ErrorLevel);

                    if (recError.ExtraData == null)
                    {
                        recError.ExtraData = new SyncErrorExtraData();
                    }
                    dynamicParameters.Add("ExtraData", JsonConvert.SerializeObject(recError.ExtraData));

                    _connection.Execute("Proc_SaveSyncDataError", dynamicParameters, commandType: CommandType.StoredProcedure);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: " + ex.Message);
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

        public static void SaveCheckpointError(string consumerName, string kinesisShardId, string seq, string err, string configDB)
        {
            using (var _connection = new MySqlConnection(configDB))
            {
                if (_connection.State == ConnectionState.Closed)
                    _connection.Open();

                try
                {
                    DynamicParameters dynamicParameters = new DynamicParameters();
                    dynamicParameters.Add("Consumer", consumerName);
                    dynamicParameters.Add("ShardId", kinesisShardId);
                    dynamicParameters.Add("SequenceNumber", seq);
                    dynamicParameters.Add("ErrorMessage", err);

                    _connection.Execute("Proc_SaveCheckpointError", dynamicParameters, commandType: CommandType.StoredProcedure);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: " + ex.Message);
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

        private static T BuildConsumerObject<T>(Amazon.DynamoDBv2.Model.Record dyanmoRecordData)
        {
            object objInstance = Activator.CreateInstance(typeof(T));


            Amazon.DynamoDBv2.Model.StreamRecord streamRecord = dyanmoRecordData.Dynamodb;
            Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> streamRecordData;
            switch (dyanmoRecordData.EventName)
            {
                case "INSERT":
                case "MODIFY":
                    streamRecordData = streamRecord.NewImage;
                    break;
                case "REMOVE":
                    streamRecordData = streamRecord.OldImage;
                    break;
                default:
                    streamRecordData = streamRecord.NewImage;
                    break;
            }
            foreach (KeyValuePair<string, Amazon.DynamoDBv2.Model.AttributeValue> entry in streamRecordData)
            {
                foreach (PropertyInfo propertyInfo in objInstance.GetType().GetProperties())
                {
                    if (propertyInfo.Name.Equals(entry.Key, StringComparison.OrdinalIgnoreCase))
                    {
                        string rawValue = GetStreamRecordAttributeValue(entry.Value);
                        if (!string.IsNullOrEmpty(rawValue))
                        {
                            KinesisDataFieldAttribute a = (KinesisDataFieldAttribute)propertyInfo.GetCustomAttribute(typeof(KinesisDataFieldAttribute), false);
                            switch (a.DataFieldType)
                            {
                                case KinesisDataFieldType.StringType:
                                    propertyInfo.SetValue(objInstance, rawValue);
                                    break;
                                case KinesisDataFieldType.IntegerType:
                                    propertyInfo.SetValue(objInstance, int.Parse(rawValue));
                                    break;
                                case KinesisDataFieldType.DecimalType:
                                    propertyInfo.SetValue(objInstance, decimal.Parse(rawValue));
                                    break;
                                case KinesisDataFieldType.DatetimeType:
                                    propertyInfo.SetValue(objInstance, DateTime.Parse(rawValue, null, System.Globalization.DateTimeStyles.RoundtripKind));
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            }
            return (T)Convert.ChangeType(objInstance, typeof(T));
        }


        /// <summary>
        /// Vào đây để xem mô tả chi tiết các kiểu dữ liệu
        /// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DotNetSDKHighLevel.html
        /// </summary>
        /// <param name="attributeValue"></param>
        /// <returns></returns>
        private static string GetStreamRecordAttributeValue(Amazon.DynamoDBv2.Model.AttributeValue attributeValue)
        {
            string result = "";

            foreach (PropertyInfo propertyInfo in attributeValue.GetType().GetProperties())
            {
                object objResult = null;
                switch (propertyInfo.Name)
                {
                    //All number types: N (number type)
                    //bool: N (number type). 0 represents false and 1 represents true.
                    case "N":
                    //All string types
                    //DateTime : The DateTime values are stored as ISO-8601 formatted strings.
                    case "S":
                        objResult = attributeValue.GetType().GetProperty(propertyInfo.Name).GetValue(attributeValue, null);
                        break;
                    default:
                        break;
                }

                if (objResult != null)
                {
                    result = objResult.ToString();
                }
                if (!string.IsNullOrEmpty(result))
                {
                    break;
                }
            }
            return result;
        }
    }
}
