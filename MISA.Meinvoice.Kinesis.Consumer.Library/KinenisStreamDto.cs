using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;
using System;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class KinenisStreamDto
    {

    }

    public class STAGING_CUSTOMER
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CUSTOMER_ID { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CUSTOMER_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ADDRESS { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string EMAIL { get; set; }
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TAX_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string BUYER_LEGAL_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int? PRIORITY { get; set; }
    }

    public class CUSTOMER_BANK_ACCOUNT
    {

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? ACCOUNT_CLOSED_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ACCOUNT_NUMBER { get; set; }

        public string CUSTOMER_CODE { get; set; }
        public DateTime? DS_PARTITION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CATEGORY { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CURRENCY { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int STATUS { get; set; }
    }

    public class COMPANY
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string COMPANY_CODE { get; set; }

        public DateTime? DS_PARTITION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string EMAIL { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string FAX { get; set; }


        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? LST_UPDATE_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string LEGAL_REPRESENT { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string MNEMONIC { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TAX_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string PHONE_NUMBER { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string NAME_ADDRESS { get; set; }
        public string NAME_LEAD_COM { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string STATUS { get; set; }

    }

    public class TRANSACTION_DATA
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ENTRY_ID { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ENTRY_TYPE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string BUYER_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string BRANCH_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string COMPANY_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string BUYER_BANK_ACCOUNT { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CURRENCY_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string PAYMENT_METHOD_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string INV_TYPE_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string INV_NOTE { get; set; }

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? TRANFER_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TRANS_NO { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? EXCHANGE_RATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ITEM_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string UNIT_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? QUANTITY { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? UNIT_PRICE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string VAT_CATEGORY_PERCENTAGE { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? VAT_AMOUNT { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? TOTAL_AMOUNT_WITHOUT_VAT { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? TOTAL_AMOUNT { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string IS_SOURCE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string MODULE { get; set; }

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? PROCESS_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? CREATION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ACCOUNT_CO_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int? PRIORITY { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string PALCAT { get; set; }

        [KinesisDataField(KinesisDataFieldType.DecimalType)]
        public decimal? AMOUNT_LCY { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int? PRODCAT { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TRANSACTION_TYPE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string REVERT_FLAG { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TRANSACTION_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ORIGIN_TRANS_REF { get; set; }

        public DateTime DS_PARTITION_DATE { get; set; }
        public DateTime? transfer_date { get; set; }

        public string ID { get; set; }
    }
}
