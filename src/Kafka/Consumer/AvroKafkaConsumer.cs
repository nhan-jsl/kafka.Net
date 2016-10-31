using Microsoft.Hadoop.Avro;
using RdKafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataMountaineer.Kafka.Consumer
{
    public class AvroKafkaConsumer<K, V> : IDisposable
    {
        private readonly int _consumerTimout;
        private readonly RdKafka.Consumer _consumer;
        private readonly IAvroSerializer<K> _keySerde = AvroSerializer.Create<K>();
        private readonly IAvroSerializer<V> _valueSerde = AvroSerializer.Create<V>();

        private readonly Dictionary<TopicPartition, TopicPartitionOffset> lastOffsets = new Dictionary<TopicPartition, TopicPartitionOffset>();

        internal AvroKafkaConsumer(RdKafka.Consumer consumer, long offset, int consumerTimeout)
        {
            _consumerTimout = consumerTimeout;
            _consumer = consumer;
            _consumer.OnPartitionsAssigned += (obj, partitions) =>
            {
                _consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p.Topic, p.Partition, offset)).ToList());
            };
        }

        public Tuple<K, V> ConsumeNext()
        {
            var next = ConsumeInternal();

            using (var keyStream = new MemoryStream(next.Key))
            using (var keyReader = new BinaryReader(keyStream))
            using (var valueStream = new MemoryStream(next.Payload))
            using (var valueReader = new BinaryReader(valueStream))
            {
                keyReader.ReadByte();
                var keySchemaId = keyReader.ReadInt32();
                var key = _keySerde.Deserialize(keyStream);

                valueReader.ReadByte();
                var valueSchemaId = valueReader.ReadInt32();
                var value = _valueSerde.Deserialize(valueStream);

                return new Tuple<K, V>(key, value);
            }
        }

        public Task CommitAsync()
        {
            return _consumer.Commit(lastOffsets.Values.ToList());
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }

        internal Message ConsumeInternal()
        {
            var msgAndError = _consumer.Consume(new TimeSpan(0, 0, 0, 0, _consumerTimout));
            if (msgAndError == null) throw new ApplicationException("message was null; timeout occurred on read");
            if (!msgAndError.HasValue) throw new ApplicationException("message has no value");
            if (msgAndError.Value.Error != ErrorCode.NO_ERROR)
            {
                throw new ApplicationException("There was an error reading the message.Error code is " + msgAndError.Value.Error);
            }
            lastOffsets[new TopicPartition { Topic = msgAndError.Value.Message.Topic, Partition = msgAndError.Value.Message.Partition }] = msgAndError.Value.Message.TopicPartitionOffset;

            return msgAndError.Value.Message;
        }
    }
}
