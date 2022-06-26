using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Amazon;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Newtonsoft.Json;
using Amazon.Runtime;
using System.Threading;
using System.Threading.Tasks;

namespace MISA.Meinvoice.Kinesis.Consumer.Library
{
    public class SecretsManager
    {
        public static string GetRDSConnectionString(string secretName)
        {
            string region = "ap-southeast-1";
            string secret = "";
            string connString = "";

            MemoryStream memoryStream = new MemoryStream();

            IAmazonSecretsManager client = new AmazonSecretsManagerClient(RegionEndpoint.GetBySystemName(region));

            GetSecretValueRequest request = new GetSecretValueRequest();
            request.SecretId = secretName;
            request.VersionStage = "AWSCURRENT"; // VersionStage defaults to AWSCURRENT if unspecified.

            GetSecretValueResponse response = null;

            // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
            // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            // We rethrow the exception by default.

            try
            {
                response = client.GetSecretValueAsync(request).Result;               
            }
            catch (DecryptionFailureException e)
            {
                // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                // Deal with the exception here, and/or rethrow at your discretion.
                Console.Error.WriteLine($"Secrets Manager can't decrypt the protected secret text using the provided KMS key-Detail: {e}");
                throw;
            }
            catch (InternalServiceErrorException e)
            {
                // An error occurred on the server side.
                // Deal with the exception here, and/or rethrow at your discretion.
                Console.Error.WriteLine($"An error occurred on the server side-Detail: {e}");
                throw;
            }
            catch (InvalidParameterException e)
            {
                // You provided an invalid value for a parameter.
                // Deal with the exception here, and/or rethrow at your discretion
                Console.Error.WriteLine($"You provided an invalid value for a parameter-Detail: {e}");
                throw;
            }
            catch (InvalidRequestException e)
            {
                // You provided a parameter value that is not valid for the current state of the resource.
                // Deal with the exception here, and/or rethrow at your discretion.
                Console.Error.WriteLine($"You provided a parameter value that is not valid for the current state of the resource-Detail: {e}");
                throw;
            }
            catch (ResourceNotFoundException e)
            {
                // We can't find the resource that you asked for.
                // Deal with the exception here, and/or rethrow at your discretion.
                Console.Error.WriteLine($"We can't find the resource that you asked for-Detail: {e}");
                throw;
            }
            catch (System.AggregateException e)
            {
                // More than one of the above exceptions were triggered.
                // Deal with the exception here, and/or rethrow at your discretion.
                Console.Error.WriteLine($"More than one of the above exceptions were triggered.-Detail: {e}");
                throw;
            }

            // Decrypts secret using the associated KMS key.
            // Depending on whether the secret is a string or binary, one of these fields will be populated.
            if (response.SecretString != null)
            {
                secret = response.SecretString;
               
            }
            else
            {
                memoryStream = response.SecretBinary;
                StreamReader reader = new StreamReader(memoryStream);
                secret = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(reader.ReadToEnd()));
            }
            if (!string.IsNullOrEmpty(secret))
            {
                RDSConfig rdsConfig = JsonConvert.DeserializeObject<RDSConfig>(response.SecretString);
                connString = $"server={rdsConfig.Host};database=tcb_meinvoice;port={rdsConfig.Port};user={rdsConfig.Username};password={rdsConfig.Password};TreatTinyAsBoolean=true;SslMode=none";
            }
            return connString;
        }
    }

    public class RDSConfig
    {
        public string Username { get; set; }
        public string Password { get; set; }
        public string Engine { get; set; }
        public string Host { get; set; }
        public string Port { get; set; }
        public string DbInstanceIdentifier { get; set; }
    }
}
