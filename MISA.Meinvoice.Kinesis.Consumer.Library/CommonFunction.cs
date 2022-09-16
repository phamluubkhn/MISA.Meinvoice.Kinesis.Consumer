using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class CommonFunction
    {
        private static readonly HttpClient client = new HttpClient();

        public static string  GetMysqlConnectionString(string apiUrl, string mysqlTemplate)
        {
            string mysqlConn = "";
            try
            {
                var response = client.GetStringAsync(apiUrl).Result;
                if (!string.IsNullOrEmpty(response))
                {
                    ApiResult apiResult = JsonConvert.DeserializeObject<ApiResult>(response);
                    if (apiResult != null && !string.IsNullOrEmpty(apiResult.Data))
                    {
                        SecretManagerData secretManagerData = JsonConvert.DeserializeObject<SecretManagerData>(apiResult.Data);
                        mysqlConn = string.Format(mysqlTemplate, secretManagerData.username, secretManagerData.password);
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("GetMysqlConnectionString Error:" +e.Message);
            }
            return mysqlConn;
        }
    }

}
