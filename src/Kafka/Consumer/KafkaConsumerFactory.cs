using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace DataMountaineer.Kafka.Consumer
{
    public static class KafkaFactory
    {
        private static readonly log4net.ILog logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public enum Offset
        {
            Beginning,
            End,
            StoredOrEnd
        }


        internal static RdKafka.Consumer GetConsumer(string topic, string kafkaBrokers, string consumerGroup, int fetchMaxWaitMs)
        {
            var config = new Config();
            config.GroupId = consumerGroup;
            config.EnableAutoCommit = false;
            config["offset.store.method"] = "broker";
            config["fetch.wait.max.ms"] = fetchMaxWaitMs.ToString();
            var consumer = new RdKafka.Consumer(config, consumerGroup);
            consumer.Subscribe(new List<String>() { topic });

            return consumer;
        }


        public static AvroKafkaConsumer<K, V> GetAvroConsumer<K, V>(string topic, string kafkaBrokers, string consumerGroup, int fetchMaxWaitMs, Offset startOffset)
        {
            long offset = 0;
            switch (startOffset)
            {
                case Offset.Beginning:
                    offset = RdKafka.Offset.Beginning;
                    break;
                case Offset.End:
                    offset = RdKafka.Offset.End;
                    break;
                case Offset.StoredOrEnd:
                    offset = RdKafka.Offset.Stored;
                    break;
            }
            var consumer = GetConsumer(topic, kafkaBrokers, consumerGroup, fetchMaxWaitMs);
            return new AvroKafkaConsumer<K, V>(consumer, offset, fetchMaxWaitMs);
        }
    }
}
