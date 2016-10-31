using Microsoft.Hadoop.Avro;
using RdKafka;
using System;
using System.IO;
using System.Threading.Tasks;

namespace DataMountaineer.Kafka.Producer
{
    public class AvroKafkaProducer<K, V> : IDisposable
    {
        private readonly RdKafka.Topic _topic;
        private readonly RdKafka.Producer _producer;

        private readonly int _keySchemaId;
        private readonly int _valueSchemaId;
        private readonly byte[] _keySchemaBytes;
        private readonly byte[] _valueSchemaBytes;

        private readonly IAvroSerializer<K> _keySerializer = AvroSerializer.Create<K>();
        private readonly IAvroSerializer<V> _valueSerializer = AvroSerializer.Create<V>();

        internal AvroKafkaProducer(RdKafka.Producer producer, RdKafka.Topic topic, int keySchemaId, int valueSchemaId)
        {
            _producer = producer;
            _topic = topic;
            _keySchemaId = keySchemaId;
            _valueSchemaId = valueSchemaId;

            _keySchemaBytes = BitConverter.GetBytes(keySchemaId);
            Array.Reverse(_keySchemaBytes);

            _valueSchemaBytes = BitConverter.GetBytes(valueSchemaId);
            Array.Reverse(_valueSchemaBytes);
        }

        public Task<DeliveryReport> SendAsync(K key, V value, int partition)
        {
            using (var keyStream = new MemoryStream())
            using (var keyWriter = new BinaryWriter(keyStream))
            using (var valueStream = new MemoryStream())
            using (var valueWriter = new BinaryWriter(valueStream))
            {
                keyWriter.Write((byte)0);
                keyWriter.Write(_keySchemaBytes);

                valueWriter.Write((byte)0);
                valueWriter.Write(_valueSchemaBytes);

                _keySerializer.Serialize(keyStream, key);
                _valueSerializer.Serialize(valueStream, value);
                return _topic.Produce(keyStream.ToArray(), valueStream.ToArray(), partition);
            }
        }


        public void Dispose()
        {
            _topic.Dispose();
            _producer.Dispose();
        }
    }
}
