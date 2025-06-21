using Kafka.Producer;
Console.WriteLine("Kafka Producer");


//var kafkaService = new KafkaService();
//var topicName = "use-case-1.1-topic";

//await kafkaService.CreateTopicAsync(topicName);

//await kafkaService.SendSimpleMessageWithNullKeyAsync(topicName);

//"------------------------------------------------------------------------------"

//var kafkaService = new KafkaService();
//var topicName = "use-case-2-topic";

//await kafkaService.CreateTopicAsync(topicName);

//await kafkaService.SendSimpleMessageWithIntKeyAsync(topicName);

//"------------------------------------------------------------------------------"


//var kafkaService = new KafkaService();
//var topicName = "use-case-3-topic";

//await kafkaService.CreateTopicAsync(topicName);

//await kafkaService.SendComplexMessageWithIntKeyAsync(topicName);


//"------------------------------------------------------------------------------"

//var kafkaService = new KafkaService();
//var topicName = "use-case-4-topic";

//await kafkaService.CreateTopicAsync(topicName);

//await kafkaService.SendMessagesToSpecificPartitionAsync(topicName);

//"------------------------------------------------------------------------------"


//var kafkaService = new KafkaService();
//var topicName = "ack-topic";

//await kafkaService.CreateTopicAsync(topicName);

//await kafkaService.SendMessageWithAckAsync(topicName);

////"------------------------------------------------------------------------------"

//var kafkaService = new KafkaService();
//var topicName = "new_cluster-topic-1";

//await kafkaService.CreateTopicWithClusterAsync(topicName);

//await kafkaService.SendMessageToClusterAsync(topicName);


//"------------------------------------------------------------------------------"

var kafkaService = new KafkaService();
var topicName = "retry-topic2";

await kafkaService.CreateTopicRetryWithClusterAsync(topicName);

await kafkaService.SendMessageToClusterWithRetryAsync(topicName);

Console.WriteLine("Messages send");