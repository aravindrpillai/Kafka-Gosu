package training.util.kafka

uses java.util.Properties
uses org.apache.kafka.clients.producer.Callback
uses org.apache.kafka.clients.producer.KafkaProducer
uses org.apache.kafka.clients.producer.ProducerConfig
uses org.apache.kafka.clients.producer.ProducerRecord
uses org.apache.kafka.clients.producer.RecordMetadata

/**
 * author : Aravind R Pillai
 * date   : 25 June 2023
 * desc   : Class to push a message
 */

public class Producer {

  private var _producer : KafkaProducer

  construct(){
    Thread.currentThread().setContextClassLoader(null)
    var properties = new Properties()
    properties.put("bootstrap.servers", Constants.KAFKA_SERVER)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.KAFKA_SERIALIZER)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.KAFKA_SERIALIZER)
    _producer = new KafkaProducer<String, String>(properties)
  }


  /**
   * function to push the message
   * @param topic
   * @param messageKey
   * @param messageValue
   * @return
   */
  public function push(topic : String, messageKey : String, messageValue : String) : boolean {
    var sent = false
    var record = new ProducerRecord<String, String>(topic, messageKey, messageValue)
    _producer.send(record, new Callback() {
      override public function onCompletion(metadata : RecordMetadata, exception : Exception) : void {
        if (exception != null) {
          print("Error producing message to Kafka: " + exception.Message)
        } else {
          print("Message sent successfully to Kafka. Topic: " + metadata.topic() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset())
          sent = true
        }
      }
    })
    //_producer.flush()
    //_producer.close()
    return sent
  }

}
