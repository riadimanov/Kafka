using Confluent.Kafka;

namespace Kafka.Consumer2
{
    internal class KafkaService
    {

        public async Task ConsumeSimpleMessageWithNullKeyAsync(string topicName)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",  // Hər consumer qrup üçün unikal olmalıdır
                AutoOffsetReset = AutoOffsetReset.Earliest  // Offset yoxdursa, əvvəldən oxu
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Subscribe(topicName);

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(millisecondsTimeout: 5000);

                    if (consumeResult != null)
                    {
                        Console.WriteLine($"Message: {consumeResult.Message.Value}");
                        Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                        Console.WriteLine("------------------------");
                    }

                    // Hər poll sonrası bir az gecikmə verək ki, polling prosesi çox intensiv olmasın
                    await Task.Delay(500);
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
            finally
            {
                consumer.Close();  // Consumer bağlanarkən qrupdan düzgün şəkildə çıxarılır
            }
        }

        public async Task ConsumeSimpleMessageWithIntKeyAsync(string topicName)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",  // Hər consumer qrup üçün unikal olmalıdır
                AutoOffsetReset = AutoOffsetReset.Earliest  // Offset yoxdursa, əvvəldən oxu
            };

            using var consumer = new ConsumerBuilder<int, string>(config).Build();

            consumer.Subscribe(topicName);

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(millisecondsTimeout: 5000);

                    if (consumeResult != null)
                    {
                        Console.WriteLine($"Message: {consumeResult.Message.Value}");
                        Console.WriteLine($"Key: {consumeResult.Message.Key}");
                        Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                        Console.WriteLine("------------------------");
                    }

                    // Hər poll sonrası bir az gecikmə verək ki, polling prosesi çox intensiv olmasın
                    await Task.Delay(500);
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
            finally
            {
                consumer.Close();  // Consumer bağlanarkən qrupdan düzgün şəkildə çıxarılır
            }
        }

    }

}

