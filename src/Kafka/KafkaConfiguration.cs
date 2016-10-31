using System;

namespace DataMountaineer.Kafka
{
    public struct KafkaConfiguration
    {
        public KafkaConfiguration(String kafkaBrokers,
            String topic,
            String groupId,
            int consumerNetworkTimeoutSeconds,
            int fetchMaxWaitMs)
        {
            if (kafkaBrokers == null || kafkaBrokers.Trim().Length == 0)
            {
                throw new ArgumentException(nameof(kafkaBrokers));
            }
            if (topic == null || topic.Trim().Length == 0)
            {
                throw new ArgumentException(nameof(topic));
            }
           
        
            KafkaBrokers = kafkaBrokers;
            Topic = topic;
            ConsumerTimeoutSeconds = consumerNetworkTimeoutSeconds;
            FetchMaxWaitMs = fetchMaxWaitMs;
        }

        public string Topic { get; set; }

        public string KafkaBrokers { get; set; }

        public int ConsumerTimeoutSeconds { get; set; }

        public int FetchMaxWaitMs { get; set; }
    }
}
