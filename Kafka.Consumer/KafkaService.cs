using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;
using Kafka.Consumer.Events;

namespace Kafka.Consumer
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
        public async Task ConsumeComplexMessageWithIntKeyAsync(string topicName)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-3-group-1",  // Hər consumer qrup üçün unikal olmalıdır
                AutoOffsetReset = AutoOffsetReset.Earliest  // Offset yoxdursa, əvvəldən oxu
            };

            using var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();

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
        public async Task ConsumeMessageFromSpecificPartitionWithNullKeyAsync(string topicName)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-3-group-1",  // Hər consumer qrup üçün unikal olmalıdır
                AutoOffsetReset = AutoOffsetReset.Earliest  // Offset yoxdursa, əvvəldən oxu
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();

            //İstədiyimiz offset-dən oxumağa başlaya bikərik.
            //consumer.Assign(new TopicPartitionOffset(topicName, new Partition(2), offset: 3));
            consumer.Assign(new TopicPartition(topicName,new Partition(2)));

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
        public async Task ConsumeMessageWithAckAsync(string topicName)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9094",
                GroupId = "ack",  // Hər consumer qrup üçün unikal olmalıdır
                AutoOffsetReset = AutoOffsetReset.Earliest , // Offset yoxdursa, əvvəldən oxu
                EnableAutoCommit = false,//true versem hecne etmeye ehtiyac yoxdu
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Assign(new TopicPartition(topicName, new Partition(2)));

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(millisecondsTimeout: 5000);

                    if (consumeResult != null)
                    {
                        try
                        {
                            Console.WriteLine($"Message: {consumeResult.Message.Value}");
                            Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                            Console.WriteLine("------------------------");

                            consumer.Commit();

                        }
                        catch(Exception e) 
                        {
                            Console.WriteLine(e);
                            throw;
                        }
                        
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
        public async Task ConsumeMessageFromClusterAsync(string topicName)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:7001,localhost:7002,localhost:7003",
                GroupId = "ack",  // Hər consumer qrup üçün unikal olmalıdır
                AutoOffsetReset = AutoOffsetReset.Earliest, // Offset yoxdursa, əvvəldən oxu
                EnableAutoCommit = false,//true versem mesajin oxunmasi haqqinda melumat vermek ucun hecne etmeye ehtiyac yoxdu
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
                        try
                        {
                            Console.WriteLine($"Message: {consumeResult.Message.Value}");
                            Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");
                            Console.WriteLine("------------------------");

                            consumer.Commit();

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            throw;
                        }

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
