package training.util.kafka

uses java.util.Properties
uses java.util.Collections
uses org.apache.log4j.Level
uses org.apache.log4j.Logger
uses gw.util.concurrent.LockingLazyVar
uses org.apache.kafka.clients.admin.NewTopic
uses org.apache.kafka.clients.admin.AdminClient
uses org.apache.kafka.clients.admin.AdminClientConfig
uses org.apache.kafka.clients.admin.ListTopicsOptions
uses org.apache.kafka.clients.admin.DeleteTopicsResult
uses org.apache.kafka.clients.admin.DeleteTopicsOptions
uses org.apache.kafka.clients.admin.DescribeTopicsOptions


/**
 * author : Aravind R Pillai
 * date   : 25 June 2023
 * desc   : Class to handle all functionalities of Kafka topic
 * usage :
 * to create a new topic
 * Topics.Instance.create("topic-name")
 * To chekc if a topic exists
 * var exists = Topics.Instance.topicExists("topic-name")
 * To get all the available topics
 * var topics = Topics.Instance.listTopics()
 */
public class Topics {

  private static var _adminClient : AdminClient
  private static var _lazyClassInstance = LockingLazyVar.make(\-> new Topics())


  private construct() {
    Logger.getLogger("org.apache.kafka.clients.admin").setLevel(Level.OFF)
    var props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER)
    if (_adminClient == null) {
      _adminClient = AdminClient.create(props)
    }
  }

  /**
   * Function to keep the class singleton
   *
   * @return
   */
  static property get Instance() : Topics {
    return _lazyClassInstance.get()
  }

  /**
   * Function to create a new topic
   *
   * @param topic
   * @param partitions
   */
  public function create(topic : String, partitions = 1) {
    if (topicExists(topic)) {
      print("Topic " + topic + " exists")
      return
    }
    var replicationFactor = 1 as short
    var newTopic = new NewTopic(topic, partitions, replicationFactor)
    _adminClient.createTopics(Collections.singletonList(newTopic))
    print("Topic created successfully.")
  }


  /**
   * Function to return all topics available
   *
   * @return
   */
  public function listTopics() : Set<String> {
    var options = new ListTopicsOptions()
    options.listInternal(false)
    var topicsResult = _adminClient.listTopics(options)
    var topicsFuture = topicsResult.listings()
    var topicListings = topicsFuture.get()
    return topicListings*.name()?.toSet()
  }


  /**
   * Function to check if a topic exists or not
   *
   * @param topic
   * @return
   */
  public function topicExists(topic : String) : boolean {
    try {
      var options = new DescribeTopicsOptions().timeoutMs(5000)
      var topicDescriptionFuture = _adminClient.describeTopics(Collections.singleton(topic), options).values().get(topic)
      return topicDescriptionFuture.get() != null
    } catch (e : Exception) {
      return false
    }
  }


  /**
   * Function to delete a topic
   * @param topic
   */
  public function deleteTopic(topic : String) {
    var deleteOptions = new DeleteTopicsOptions()
    deleteOptions.timeoutMs(5000)
    var deleteResult = _adminClient.deleteTopics(Collections.singleton(topic), deleteOptions)
    deleteResult.all().get()
    print("Topic deleted successfully: " + topic)
  }



  /**
   * Function to delete all topics
   */
  public function deleteAllTopics() {
    foreach (topic in listTopics()){
      deleteTopic(topic)
    }
  }



  /**
   * Function to close the client
   */
  public function closeClient() {
    if (_adminClient != null) {
      _adminClient.close()
    }
  }


}
