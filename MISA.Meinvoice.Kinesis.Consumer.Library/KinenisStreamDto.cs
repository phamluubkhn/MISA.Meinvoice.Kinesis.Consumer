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
        [JsonProperty(Required = Required.Always)]
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

        public DateTime? DS_PARTITION_DATE { get; set; }
    }

    public class CUSTOMER_BANK_ACCOUNT
    {

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? ACCOUNT_CLOSED_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ACCOUNT_NUMBER { get; set; }

        [JsonProperty(Required = Required.Always)]
        public string CUSTOMER_CODE { get; set; }
        //public DateTime? DS_PARTITION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CATEGORY { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CURRENCY { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int STATUS { get; set; }

        public DateTime? DS_PARTITION_DATE { get; set; }
    }

    public class COMPANY
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        [JsonProperty(Required = Required.Always)]
        public string COMPANY_CODE { get; set; }

        //public DateTime? DS_PARTITION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string EMAIL { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string FAX { get; set; }


        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? LST_UPDATE_DATE { get; set; }

        public DateTime? DS_PARTITION_DATE { get; set; }

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

        public string ADDRESS { get; set; }
        public string NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string STATUS { get; set; }

    }

    public class TRANSACTION_DATA
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        [JsonProperty(Required = Required.Always)]
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
        public DateTime? TRANSFER_DATE { get; set; }

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

        //public DateTime DS_PARTITION_DATE { get; set; }
        //public DateTime? transfer_date { get; set; }

        //public string ID { get; set; }
    }

    public class Pl01gtgt
    {



        //        ds_partition_date DATE
        //contract_number Số bút toán VARCHAR2(255 BYTE)
        /// <summary>
        /// Số bút toán
        /// </summary>
        [JsonProperty(Required = Required.Always)]
        public string contract_number { get; set; }

        //COMPANY Mã chi nhánh VARCHAR2(255 BYTE)
        /// <summary>
        /// Mã chi nhánh
        /// </summary>
        public string COMPANY { get; set; }

        //pl_category Mã PL NUMBER
        /// <summary>
        /// Mã PL
        /// </summary>
        public int? pl_category { get; set; }

        //booking_date Ngày hạch toán DATE
        /// <summary>
        /// Ngày hạch toán
        /// </summary>
        public DateTime? booking_date { get; set; }

        //amount Số tiền quy đổi NUMBER
        /// <summary>
        /// Số tiền quy đổi
        /// </summary>
        public Decimal? amount { get; set; }

        //description Diễn giải VARCHAR2(4000 BYTE)
        /// <summary>
        /// Diễn giải
        /// </summary>
        public string description { get; set; }

        public int? TYPE_CODE { get; set; }
        //PURPOSE Mục đích của sản phẩm cho vay NUMBER
        /// <summary>
        /// Mục đích của sản phẩm cho vay
        /// </summary>
        public int? PURPOSE { get; set; }

        //product_category Mã sản phẩm NUMBER
        /// <summary>
        /// Mã sản phẩm
        /// </summary>
        public int? product_category { get; set; }

        //currency Loại tiền VARCHAR2(255 BYTE)
        /// <summary>
        /// Loại tiền
        /// </summary>
        public string currency { get; set; }

        //amount_foreign_currency Số tiền nguyên tệ NUMBER
        /// <summary>
        ///  Số tiền nguyên tệ
        /// </summary>
        public decimal? amount_foreign_currency { get; set; }

        //TRANSACTION_CODE Mã giao dịch VARCHAR2(255 BYTE)
        /// <summary>
        /// Mã giao dịch
        /// </summary>
        public string TRANSACTION_CODE { get; set; }

        //SYSTEM_ID Mã Hệ thống VARCHAR2(255 BYTE)
        /// <summary>
        /// Mã Hệ thống
        /// </summary>
        public string SYSTEM_ID { get; set; }

        //customer_code Mã khách hàng VARCHAR2(30 BYTE)
        /// <summary>
        /// Mã khách hàng
        /// </summary>
        public string customer_code { get; set; }

        //COMPANY_CODE Mã chi nhánh VARCHAR2(255 BYTE)
        /// <summary>
        /// Mã chi nhánh
        /// </summary>
        public string COMPANY_CODE { get; set; }

        //SOURCE_ID Mã categ.entry VARCHAR2(255 BYTE)
        /// <summary>
        /// Mã categ.entry
        /// </summary>
        public string SOURCE_ID { get; set; }

        //VALUE_DATE Ngày giá trị DATE
        /// <summary>
        /// Ngày giá trị
        /// </summary>
        public DateTime? VALUE_DATE { get; set; }

        //REVERSAL_MARKER Trường đánh dấu giao dịch Revert CHAR(1 BYTE)
        /// <summary>
        /// Trường đánh dấu giao dịch Revert
        /// </summary>
        public string REVERSAL_MARKER { get; set; } 
    }

    public class CURRENCY
    {
        [JsonProperty(Required = Required.Always)]
        public string currency_code { get; set; }
        public string curr_no { get; set; }
        public decimal? sell_rate { get; set; }
        public decimal? buy_rate { get; set; }
        public decimal? mid_reval_rate { get; set; }
        public DateTime? process_date { get; set; }
        public string ds_partition_date { get; set; }
    }

    public class END_OF_FILE
    {
        public string msg_type { get; set; }

        public END_OF_FILE_DETAILS details { get; set; }
    }

    public class END_OF_FILE_DETAILS
    {
        public string module_name { get; set; }
        public string is_source { get; set; }
        public int produced { get; set; }
        public int skipped { get; set; }
        public int total { get; set; }
        public DateTime __pushed_at { get; set; }
    }

    public class ApiResult
    {
        public bool Success { get; set; }
        public string ErrorCode { get; set; }
        public string Data { get; set; }
    }

    public class SecretManagerData
    {
        public string username { get; set; }
        public string password { get; set; }
    }
}
