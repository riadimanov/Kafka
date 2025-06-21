using Kafka.Consumer2;

Console.WriteLine("Kafka Consumer 2");


var kafkaService = new KafkaService();
var topicName = "use-case-1.1-topic";

await kafkaService.ConsumeSimpleMessageWithNullKeyAsync(topicName);

Console.ReadKey();