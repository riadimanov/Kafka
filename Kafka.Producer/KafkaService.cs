using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Kafka.Producer.Events;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        public async Task CreateTopicAsync(string topicName)
        {
            // AdminClientBuilder vasitəsilə Kafka brokerinə qoşuluruq.
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                // Birdən çox broker olduqda onları massiv kimi göstərmək mümkündür.
                // Hal-hazırda yalnız bir broker istifadə olunur.
                BootstrapServers = "localhost:9094"
            }).Build();

            try
            {
                // Yeni bir mövzu yaratmaq üçün CreateTopicsAsync metodu çağırılır.
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,       // Mövzu adı
                        NumPartitions = 3,      // Partisiya sayı
                        ReplicationFactor = 1   // Replikasiya faktoru
                    }
                });

                Console.WriteLine($"Topic with name {topicName} created successfully!");
            }

            catch (CreateTopicsException e)
            {
                // Əgər mövzu artıq mövcuddursa və ya başqa xəta baş verərsə, bu mesaj konsolda göstəriləcək.
                Console.WriteLine($"An error occurred creating topic {topicName}: {e.Results[0].Error.Reason}");
            }

            catch (Exception e)
            {
                Console.WriteLine($"Unexpected error: {e.Message}");
            }
        }

        public async Task CreateTopicWithClusterAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                // Birdən çox broker olduqda onları massiv kimi göstərmək mümkündür.
                BootstrapServers = "localhost:7001,localhost:7002,localhost:7003"//birinde versek bes edir amma o biri cokse digerlerine muraciet ede bilmeyeceyik buna gorede hamisin yazmaq melsehetdi
            }).Build();

            try
            {
                // Yeni bir topic yaratmaq üçün CreateTopicsAsync metodu çağırılır.
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,       // Mövzu adı
                        NumPartitions = 3,      // Partisiya sayı
                        ReplicationFactor = 3   // Replikasiya faktoru
                    }
                });

                Console.WriteLine($"Topic with name {topicName} created successfully!");
            }

            catch (CreateTopicsException e)
            {
                // Əgər mövzu artıq mövcuddursa və ya başqa xəta baş verərsə, bu mesaj konsolda göstəriləcək.
                Console.WriteLine($"An error occurred creating topic {topicName}: {e.Results[0].Error.Reason}");
            }

            catch (Exception e)
            {
                Console.WriteLine($"Unexpected error: {e.Message}");
            }
        }

        public async Task CreateTopicRetryWithClusterAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                // Birdən çox broker olduqda onları massiv kimi göstərmək mümkündür.
                BootstrapServers = "localhost:7001,localhost:7002,localhost:7003"//birinde versek bes edir amma o biri cokse digerlerine muraciet ede bilmeyeceyik buna gorede hamisin yazmaq melsehetdi
            }).Build();

            try
            {
                var configs = new Dictionary<string, string>()
                {
                    {"min.insync.replicas","2"} // yazılan mesajın etibarlı sayılması üçün(yeni ack true almaq ucun)
                                                // replikasiya olunan nüsxələrin (replicaların) neçə dənəsinin aktiv və sinxron vəziyyətdə olması
                                                // lazım olduğunu təyin edir. Bu dəyər 3 olaraq təyin edildikdə, Kafka brokeri mesajın 
                                                // yalnız 3 replica aktiv olduqda ve mesaj her birine yazildiqdan sonra mesajin yazılmasını təsdiqləyəcək.
                };
                // Yeni bir topic yaratmaq üçün CreateTopicsAsync metodu çağırılır.
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,       // Mövzu adı
                        NumPartitions = 3,      // Partisiya sayı
                        ReplicationFactor = 3,  // Replikasiya faktoru
                        Configs = configs
                    }
                });

                Console.WriteLine($"Topic with name {topicName} created successfully!");
            }

            catch (CreateTopicsException e)
            {
                // Əgər mövzu artıq mövcuddursa və ya başqa xəta baş verərsə, bu mesaj konsolda göstəriləcək.
                Console.WriteLine($"An error occurred creating topic {topicName}: {e.Results[0].Error.Reason}");
            }

            catch (Exception e)
            {
                Console.WriteLine($"Unexpected error: {e.Message}");
            }
        }

        public async Task SendSimpleMessageWithNullKeyAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 1000))
            {
                Null key = null;

                var message = new Message<Null, string>()
                {
                    Value = $"Message(use-case-1) {item}"
                };

                try
                {
                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicName, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }

        public async Task SendSimpleMessageWithIntKeyAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<int, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 100))
            {

                var message = new Message<int, string>()
                {
                    Value = $"Message(use-case-1) {item}",
                    Key = item
                };

                try
                {
                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicName, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }

        public async Task SendComplexMessageWithIntKeyAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config).SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();

            foreach (var item in Enumerable.Range(1, 100))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item
                };

                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item
                };

                try
                {
                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicName, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }

        public async Task SendMessagesToSpecificPartitionAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 1000))
            {
                Null key = null;

                var message = new Message<Null, string>()
                {
                    Value = $"Message(use-case-4) {item}"
                };

                try
                {
                    var topicPartition = new TopicPartition(topicName, new Partition(2));

                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicPartition, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }

        public async Task SendMessageWithAckAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7001,localhost:7002,localhost:7003",
                Acks = 0,// 1,-1,-1 all demekdir
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 1000))
            {
                Null key = null;

                var message = new Message<Null, string>()
                {
                    Value = $"Message(use-case-1) {item}"
                };

                try
                {
                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicName, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }

        public async Task SendMessageToClusterAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7001,localhost:7002,localhost:7003",
                Acks = Acks.All,// 1,-1,-1 all demekdir
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 1000))
            {
                Null key = null;

                var message = new Message<Null, string>()
                {
                    Value = $"Message(use-case-1) {item}"
                };

                try
                {
                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicName, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }

        public async Task SendMessageToClusterWithRetryAsync(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7001,localhost:7002,localhost:7003",
                Acks = Acks.All,// 1,-1,-1 all demekdir
            };

            // Primitive tiplər library tərəfindən avtomatik serialize edilir.
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 1000))
            {
                Null key = null;

                var message = new Message<Null, string>()
                {
                    Value = $"Message(use-case-1) {item}"
                };

                try
                {
                    // Mesajı Kafka-ya göndəririk və nəticəni alırıq
                    var result = await producer.ProduceAsync(topicName, message);

                    // Mesajın göndərilməsi ilə bağlı məlumatı göstəririk
                    Console.WriteLine($"Message '{message.Value}' delivered to topic '{result.Topic}', partition {result.Partition}, offset {result.Offset}");

                    //Bu hisssəni sadəcə olaraq cavabın strukturunu görmək üçün yazmışam
                    if (item == 10)
                    {
                        foreach (var propertyInfo in result.GetType().GetProperties())
                        {
                            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                        }
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message due to Kafka error: {e.Error.Reason}");
                }
                catch (Exception ex)
                {

                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }

                Console.WriteLine("-------------------------------");
                await Task.Delay(200);
            }
        }
    }
}
