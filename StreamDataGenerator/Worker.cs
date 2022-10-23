using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

namespace StreamDataGenerator
{
    public class Worker : BackgroundService
    {
        private readonly StreamWriter _streamWriter;
        private readonly ILogger<Worker> _logger;

        public Worker(StreamWriter streamWriter, ILogger<Worker> logger)
        {
            _streamWriter = streamWriter;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await produceRandomUserTxAsync("test-topic", stoppingToken);                
        }

        private async Task produceRandomUserTxAsync(string topic, CancellationToken stoppingToken)
        {
            Random random = new Random();
            int i = 0;

            while (!stoppingToken.IsCancellationRequested)
            {
                string userId = getRandomString(random, 15);
                int amount = random.Next(0, 1000);
                DateTime date = getRandomDateInRange(random, new DateTime(2020, 1, 1), new DateTime(2020, 12, 31));

                UserTx userTx = new UserTx { UserId = userId, TxDate = date, TotalAmount = amount };
                string message = JsonConvert.SerializeObject(userTx);

                await _streamWriter.Write(userId, message, topic);

                _logger.LogInformation("Produced messages {Count}", i++);
            }
        }

        private static string getRandomString(Random random, int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private static DateTime getRandomDateInRange(Random random, DateTime start, DateTime end)
        {
            TimeSpan timeSpan = end - start;
            TimeSpan newSpan = new TimeSpan(0, random.Next(0, (int)timeSpan.TotalMinutes), 0);
            return start + newSpan;
        }
    }
}
