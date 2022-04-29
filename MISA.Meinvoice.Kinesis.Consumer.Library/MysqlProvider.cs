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
            Console.Error.WriteLine("CheckHealth mysql start");
            try
            {
                using (var connection = new MySqlConnection(connectionString))
                {
                    if (connection.State == ConnectionState.Closed)
                        connection.Open();
                    return connection.Ping();
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("CheckHealth mysql ex" + ex.Message);
                return false;
            }
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

        public static void SyncBatchData(List<Record> rec, string configDB, string application, int maxErrorCountToShutDown)
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
                        if (errorCount >= maxErrorCountToShutDown)
                        {
                            RecordProcessorEntity recordProcessorEntity = MysqlProvider.BuildRecordLogData(item, application, SyncDataErrorLevel.StopConsumer, $"{application} Reach Max Error Count To ShutDown");
                            SaveSyncDataError(recordProcessorEntity, configDB);
                            throw new Exception($"{application} Reach Max Error Count To ShutDown");
                        }
                        bool isError = false;
                        try
                        {
                            //Console.WriteLine($"Execute Start {DateTime.Now.Second}- {DateTime.Now.Millisecond} for {item.SequenceNumber} " );
                            DynamicParameters dynamicParameters = new DynamicParameters();
                            string procedureName = BuildSyncDataParam(item.Data, application, dynamicParameters);
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



        private static string BuildSyncDataParam(byte[] data, string application, DynamicParameters dynamicParameters)
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
                            dynamicParameters.Add("CompanyName", company.NAME_LEAD_COM);
                            dynamicParameters.Add("Status", company.STATUS);
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
                            dynamicParameters.Add("TRANFER_DATE", transaction.TRANFER_DATE);
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
                            procedureName = "Proc_SyncTransactionData";
                        }
                        else
                        {
                            throw new Exception("Deserialize TRANSACTION_DATA Fails");
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
