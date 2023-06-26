package training.util.kafka


/**
 * author : Aravind R Pillai
 * date   : 25 June 2023
 * Desc   : Test class
 */
class KafkaTest {

  private static var consumerThread : Thread


  /**
   * Function to create the topi and start pushing messages
   */
  static function createTopicAndPushMessage(){
    print("Flushing all topics..")
    Topics.Instance.deleteAllTopics()

    print("\nCreating topic 1")
    var topic1= "topic-100"
    Topics.Instance.create(topic1)

    print("\nCreating topic 2")
    var topic2= "topic-200"
    Topics.Instance.create(topic2)

    var p = new Producer()
    //topic 1
    p.push(topic1, "message_key_1_1", "message__value__1__1")
    p.push(topic1, "message_key_1_2", "message__value__1__2")
    p.push(topic1, "message_key_1_3", "message__value__1__3")
    //topic 2
    p.push(topic2, "message_key_2_1", "message__value__2__1")
    p.push(topic2, "message_key_2_2", "message__value__2__2")
    p.push(topic2, "message_key_2_3", "message__value__2__3")
  }


  /**
   * Function to start the consumer polling
   */
  static function startConsumer(){
    var consumerGroupID = "consumer_group_1"
    print("Consumer Starting Request Received")
    consumerThread = new Thread(new Consumer(consumerGroupID))
    consumerThread.setName(consumerGroupID)
    consumerThread.start()
    print("Consumer Thread -- " + consumerThread.Name)
    print("Consumer Polling Started")
  }


  /**
   * Function to stop consumer polling
   */
  static function stopConsumer(){
    print("Consumer Polling Termination Request Received")
    print("Consumer Thread Name :: " + consumerThread?.Name)
    if(consumerThread != null) {
      consumerThread.interrupt()
    }
    print("Consumer Polling Terminated")
  }

}