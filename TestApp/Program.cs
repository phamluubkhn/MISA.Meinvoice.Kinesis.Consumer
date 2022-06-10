using MISA.Meinvoice.Kinesis.Consumer.Library;
using System;
using Amazon.Kinesis.ClientLibrary;
using System.Collections.Generic;
using MySqlConnector;
using Newtonsoft.Json;
using System.Text;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            List<Record> records = new List<Record>();

            SyncBatchTransactionData();
            Console.WriteLine("Hello World!");
        }
         
        public static bool SyncBatchTransactionData()
        {

            bool result = true;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("INSERT INTO trans_data_temp  VALUES ");
            List<string> rows = new List<string>();
            for (int i = 0; i < 1; i++)
            {
                string rowText = ProcessTransactionRecord(i.ToString());
                if (!string.IsNullOrEmpty(rowText))
                {
                    rows.Add(rowText);
                }
            }
            stringBuilder.Append(string.Join(",", rows));
            stringBuilder.Append(";");
            var x = stringBuilder.ToString();
            return result;
        }


        private static string ProcessTransactionRecord( string count)
        {

            string recordData = "{\"id\": \"d0c86c67-24d5-47b2-a523-8739db301bc2\", \"entry_id\": \"197182236341724.010001\", \"entry_type\": \"T24_CATEG\", \"buyer_code\": null, \"branch_code\": \"VN0010260\", \"buyer_bank_account\": \"USD1112100010001\", \"currency_code\": \"USD\", \"inv_type_code\": \"1K00D\", \"payment_method_name\": \"CK\", \"inv_note\": \"REBATE DICH VU TTTM\", \"transfer_date\": \"2021-12-25\", \"trans_no\": \"BC213590033900129\", \"exchange_rate\": \"22900.000000\", \"item_name\": null, \"unit_name\": null, \"quantity\": null, \"unit_price\": null, \"vat_category_percentage\": \"KCT\", \"vat_amount\": null, \"total_amount_without_vat\": \"24.650000\", \"total_amount\": \"24.650000\", \"is_source\": \"T24\", \"module\": \"T24_BC\", \"process_date\": \"2021-12-25\", \"creation_date\": \"2022-05-11\", \"account_co_code\": null, \"priority\": null, \"palcat\": \"52178\", \"amount_lcy\": \"24.650000\", \"prodcat\": \"-1\", \"transaction_type\": \"CHG\", \"revert_flag\": null, \"transaction_code\": \"51\", \"origin_trans_ref\": \"BC213590033900129\", \"ds_partition_date\": \"2021-12-25\"}";
            TRANSACTION_DATA transaction = JsonConvert.DeserializeObject<TRANSACTION_DATA>(recordData);
            string result = "";
        
            result = string.Format("(UUID(),{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32},{33}, now())",
                    transaction.ENTRY_ID == null ? "NULL" : $"'{MySqlHelper.EscapeString(transaction.ENTRY_ID)}'" ,
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
                    "1"); ;
            string xxx = "{\"id\": \"d0c86c67-24d5-47b2-a523-8739db301bc2\", \"entry_id\": \"197182236341724.010001\", \"entry_type\": \"T24_CATEG\", \"buyer_code\": null, \"branch_code\": \"VN0010260\", \"buyer_bank_account\": \"USD1112100010001\", \"currency_code\": \"USD\", \"inv_type_code\": \"1K00D\", \"payment_method_name\": \"CK\", \"inv_note\": \"REBATE DICH VU TTTM\", \"transfer_date\": \"2021-12-25\", \"trans_no\": \"BC213590033900129\", \"exchange_rate\": \"22900.000000\", \"item_name\": null, \"unit_name\": null, \"quantity\": null, \"unit_price\": null, \"vat_category_percentage\": \"KCT\", \"vat_amount\": null, \"total_amount_without_vat\": \"24.650000\", \"total_amount\": \"24.650000\", \"is_source\": \"T24\", \"module\": \"T24_BC\", \"process_date\": \"2021-12-25\", \"creation_date\": \"2022-05-11\", \"account_co_code\": null, \"priority\": null, \"palcat\": \"52178\", \"amount_lcy\": \"24.650000\", \"prodcat\": \"-1\", \"transaction_type\": \"CHG\", \"revert_flag\": null, \"transaction_code\": \"51\", \"origin_trans_ref\": \"BC213590033900129\", \"ds_partition_date\": \"2021-12-25\"}";
            Pl01gtgt pl01Gtgt = JsonConvert.DeserializeObject<Pl01gtgt>(xxx);
            result = string.Format("({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18})",
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
     "0",
    pl01Gtgt.COMPANY_CODE == null ? "NULL" : $"'{MySqlHelper.EscapeString(pl01Gtgt.COMPANY_CODE)}'",
    "1");
            return result;
        }
    }
}
