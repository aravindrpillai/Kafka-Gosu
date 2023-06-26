package training.util.kafka

uses java.time.Duration
uses java.util.Properties
uses org.apache.kafka.clients.consumer.KafkaConsumer
uses org.apache.kafka.clients.consumer.ConsumerConfig
uses org.apache.kafka.common.errors.InterruptException


/**
 * author : Aravind R Pillai
 * date   : 25 June 2023
 * desc   : Class to consume the messages
 */
public class Consumer implements Runnable{

  private var _consumer : KafkaConsumer

  construct(consumerGroupID :  String) {
    print("Starting to poll")
    Thread.currentThread().setContextClassLoader(null)
    var properties = new Properties()
    properties.put("bootstrap.servers", training.util.kafka.Constants.KAFKA_SERVER)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, training.util.kafka.Constants.KAFKA_DESERIALIZER)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, training.util.kafka.Constants.KAFKA_DESERIALIZER)

    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000); // 60 Seconds
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000); // 60 seconds
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000); // 3 seconds


    properties.put("group.id", consumerGroupID)
    _consumer = new KafkaConsumer<String, String>(properties)
    var topics = training.util.kafka.Topics.Instance.listTopics()
    print("Found ${topics.Count} topic(s) - ${topics.toList()}")
    _consumer.subscribe(Collections.singleton("topic-200"))
  }

  /**
   * Function to poll each topic
   * @param topic
   */
  override function run() {
    print("starting run()")
    try {
      while (not Thread.interrupted()) {
        var records = _consumer.poll(Duration.ofSeconds(2))
        for (record in records) {
          print("Received message: Topic : " + record.topic() + " || Key : " + record.key() + " || Value : " + record.value())
        }
      }
    }catch(e: InterruptException){
      print("Consumer thread interupted...!!")
    }finally {
      _consumer.commitSync()
      _consumer.close()
    }
  }


}
