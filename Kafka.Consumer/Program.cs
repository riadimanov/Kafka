using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");


//var kafkaService = new KafkaService();
//var topicName = "use-case-1.1-topic";

//await kafkaService.ConsumeSimpleMessageWithNullKeyAsync(topicName);

//----------------------------------------------------------------------------

//var kafkaService = new KafkaService();
//var topicName = "use-case-2-topic";

//await kafkaService.ConsumeSimpleMessageWithIntKeyAsync(topicName);

//----------------------------------------------------------------------------


//var kafkaService = new KafkaService();
//var topicName = "use-case-4-topic";

//await kafkaService.ConsumeMessageFromSpecificPartitionWithNullKeyAsync(topicName);


//----------------------------------------------------------------------------

var kafkaService = new KafkaService();
var topicName = "new_cluster-topic-1";

await kafkaService.ConsumeMessageFromClusterAsync(topicName);

Console.ReadKey();