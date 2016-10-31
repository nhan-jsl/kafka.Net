using Microsoft.Hadoop.Avro;
using Newtonsoft.Json;

namespace DataMountaineer.Kafka
{
    public static class Avro
    {
        public static string GenerateSchema<T>()
        {
            return JsonConvert.SerializeObject(
                new
                {
                    schema = AvroSerializer.Create<T>().ReaderSchema.ToString()
                });
        }
    }
}
