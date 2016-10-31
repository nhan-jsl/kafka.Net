using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace DataMountaineer.Kafka
{
    public class SchemaRegistry
    {
        private static readonly log4net.ILog logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private const string SchemaContentType = "application/vnd.schemaregistry.v1+json";
        
        public SchemaRegistry(String schemaRegistry)
        {
            if (schemaRegistry == null || schemaRegistry.Trim().Length == 0)
            {
                throw new ArgumentException(nameof(schemaRegistry));
            }
            SchemaRegistryEndpoint = schemaRegistry;
        }

        public String SchemaRegistryEndpoint
        {
            get;
            private set;
        }


        public async Task RegisterSchema(string topic, string schema, bool isKey = false)
        {
            var content = new StringContent(schema, UTF8Encoding.UTF8, SchemaContentType);
            using (var client = new HttpClient())
            {
                var message = await client.PostAsync("http://" + SchemaRegistryEndpoint + "/subjects/" + GetSchemaRegistryTopic(topic, isKey) + "/versions", content);
                if (!message.IsSuccessStatusCode)
                {
                    throw new ApplicationException("call to schema registry failed");
                }
            }
        }

        public async Task<bool> IsSchemaCompatible(string topic, string schema, bool isKey = false)
        {
            var content = new StringContent(schema, UTF8Encoding.UTF8, SchemaContentType);
            using (var client = new HttpClient())
            {
                var message = await client.PostAsync("http://" + SchemaRegistryEndpoint + "/compatibility/subjects/" + GetSchemaRegistryTopic(topic, isKey) + "/versions/latest", content);
                if (!message.IsSuccessStatusCode)
                {
                    throw new ApplicationException("call to schema registry failed");
                }
                var result = JsonConvert.DeserializeObject<SchemaCompatibilityResponse>(await message.Content.ReadAsStringAsync());
                return result.is_compatible;
            }
        }

        public async Task<int> GetSchemaId(string topic, bool isKey = false)
        {
            using (var client = new HttpClient())
            {
                var message = await client.GetAsync("http://" + SchemaRegistryEndpoint + "/subjects/" + GetSchemaRegistryTopic(topic, isKey) + "/versions/latest");
                if (!message.IsSuccessStatusCode)
                {
                    throw new ApplicationException("call to schema registry failed");
                }
                var result = JsonConvert.DeserializeObject<SchemaResponse>(await message.Content.ReadAsStringAsync());
                return result.id;
            }
        }

        static string GetSchemaRegistryTopic(string topic, bool isKey)
        {
            if (isKey)
            {
                return topic + "-key";
            }
            return topic + "value";
        }
    }

}
