using System;
using System.Threading.Tasks;

namespace DataMountaineer.Kafka.Producer
{
    public static class KafkaProducerFactory
    {
        private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static public async Task<AvroKafkaProducer<K, T>> GetProducer<K, T>(KafkaConfiguration configuration, string schemaRegistryEndPoint)
        {
            var schemaRegistry = new SchemaRegistry(schemaRegistryEndPoint);

            var keySchema = Avro.GenerateSchema<K>();
            await schemaRegistry.RegisterSchema(configuration.Topic, keySchema, true);

            var valueSchema = Avro.GenerateSchema<T>();
            if (!await schemaRegistry.IsSchemaCompatible(configuration.Topic, valueSchema))
                throw new ApplicationException("value schema is not compatible");

            var keySchemaId = await schemaRegistry.GetSchemaId(configuration.Topic, true);
            var valueSchemaId = await schemaRegistry.GetSchemaId(configuration.Topic);

            Logger.Info("Creating Kafka Producer for topic: " + configuration.Topic);
            var producer = new RdKafka.Producer(configuration.KafkaBrokers);
            var topic = producer.Topic(configuration.Topic);
            return new AvroKafkaProducer<K, T>(producer, topic, keySchemaId, valueSchemaId);
        }
    }
}

