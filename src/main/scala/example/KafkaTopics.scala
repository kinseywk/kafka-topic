package example

import scala.collection.JavaConverters._
import scala.util.control._
import scala.util.control.Breaks._
import scala.util.{Try, Success, Failure}
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions, NewTopic}

object KafkaTopics extends App {
  var adminClient: Option[AdminClient] = None

  var topics = List("foo", "bar", "qux")
  init()
  println(listTopics())
  createTopics(topics)
  Thread.sleep(2000)
  println(listTopics())
  deleteTopics(topics)
  println(listTopics())
  cleanup()
  
  def init(host: String = "sandbox-hdp.hortonworks.com", port: Int = 6667){
    if(adminClient.isEmpty) {
      val props: Properties = new Properties()

      props.put("bootstrap.servers", s"$host:$port")
      adminClient = Some(AdminClient.create(props))
      println("Connected to Kafka server as administrator")
      /*
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("acks", "all")
      */
    } else {
      println("Error: Already connected to Kafka")
    }
  }
  
  def cleanup() = {
    if(adminClient.isDefined) {
      adminClient.get.close()
      println("Disconnected from Kafka")
    }
  }

  def listTopics(listInternal: Boolean = false, timeout: Int = 500): java.util.Set[String] = {
    if(adminClient.isDefined) {
      adminClient.get.listTopics((new ListTopicsOptions()).listInternal(listInternal).timeoutMs(timeout)).names.get
    } else {
      println("Error: Not connected to Kafka")
      null
    }
  }

  def topicExists(topic: String): Boolean = {
    if(adminClient.isDefined) {
      listTopics().contains(topic)
    } else {
      println("Error: Not connected to Kafka")
      false
    }
  }

  def createTopics(topics: Seq[String]): Unit = {
    topics.foreach(x => createTopic(x))
  }

  def createTopic(topic: String, partitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    var result = false

    if(!topicExists(topic)) {
      val createTopicsResult = adminClient.get.createTopics(java.util.Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))

      var createTopicsResultIterator = createTopicsResult.values.entrySet.iterator
      while(createTopicsResultIterator.hasNext) {
        val entry = createTopicsResultIterator.next

        Try {
          println(s"Topic '${entry.getKey}' created successfully")
        } match {
          case Success(_) => {}
          case Failure(exception) => println(exception)
        }
      }

      result = true
    } else {
      println(s"Error: Topic '$topic' already exists")
    }

    result
  }

  def deleteTopics(topics: Seq[String]): Unit = {
    topics.foreach(x => deleteTopic(x))
  }

  def deleteTopic(topic: String): Boolean = {
    var result = false

    if(topicExists(topic)) {
      val deleteTopicsResult = adminClient.get.deleteTopics(java.util.Collections.singletonList(topic))

      var deleteTopicsResultIterator = deleteTopicsResult.values.entrySet.iterator
      while(deleteTopicsResultIterator.hasNext) {
        val entry = deleteTopicsResultIterator.next

        Try {
          println(s"Topic '${entry.getKey}' deleted")
        } match {
          case Success(_) => {}
          case Failure(exception) => println(exception)
        }
      }

      result = true
    } else {
      println(s"Error: Topic '$topic' not found")
    }

    result
  }
}