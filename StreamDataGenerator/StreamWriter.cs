using System.Threading.Tasks;

using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace StreamDataGenerator
{
    public class StreamWriter
    {
        private readonly ILogger<StreamWriter> _logger;

        public StreamWriter(ProducerConfig producerConfig, ILogger<StreamWriter> logger)
        {
            _logger = logger;
            Config = producerConfig;
        }

        public ProducerConfig Config { get; }

        public async Task Write(string key, string message, string topic)
        {
            using var producer = new ProducerBuilder<string, string>(Config).Build();

            var dr = await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = message });

            _logger.LogInformation("Produced message to {Topic} partition {Partition} offset {Offset} status {Status}", dr.Topic, dr.Partition, dr.Offset, dr.Status);
        }
    }
}
