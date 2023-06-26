package training.util.kafka

/**
 * Class to hold the costants
 */
class Constants {

  public static var _kafkaServer : String as KAFKA_SERVER = "localhost:9092"

  public static var _kafkaSerializer : String as KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  public static var _kafkaDeSerializer : String as KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer"



}