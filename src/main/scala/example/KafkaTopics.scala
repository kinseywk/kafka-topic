package example

import scala.collection.JavaConverters._
import scala.util.control._
import scala.util.control.Breaks._
import scala.util.{Try, Success, Failure}
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import org.ekrich.config._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.KafkaConsumer

import org.apache.log4j.{Level, Logger}

object KafkaTopics {
  def listTopics(adminClient: AdminClient, listInternal: Boolean = false, timeout: Int = 500): java.util.Set[String] = {
    adminClient.listTopics((new ListTopicsOptions()).listInternal(listInternal).timeoutMs(timeout)).names.get
  }

  def topicExists(adminClient: AdminClient, topic: String): Boolean = {
    listTopics(adminClient).contains(topic)
  }

  def createTopic(adminClient: AdminClient, topic: String, partitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    var result = false

    if(!topicExists(adminClient, topic)) {
      val createTopicsResult = adminClient.createTopics(java.util.Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))

      var createTopicsResultIterator = createTopicsResult.values.entrySet.iterator
      while(createTopicsResultIterator.hasNext) {
        val entry = createTopicsResultIterator.next

        Try {
          //entry.getValue.thenApply((x) => println(x))
          println(s"${entry.getKey}: ${entry.getValue.get(2, TimeUnit.SECONDS)}")
        } match {
          case Success(_) => {}
          case Failure(exception) => println(exception)
        }
      }

      result = true
    } else {
      println(s"Error: Topic $topic already exists")
    }

    result
  }

  def deleteTopic(adminClient: AdminClient, topic: String): Boolean = {
    var result = false

    if(topicExists(adminClient, topic)) {
      val deleteTopicsResult = adminClient.deleteTopics(java.util.Collections.singletonList(topic))

      var deleteTopicsResultIterator = deleteTopicsResult.values.entrySet.iterator
      while(deleteTopicsResultIterator.hasNext) {
        val entry = deleteTopicsResultIterator.next

        Try {
          println(s"${entry.getKey}: ${entry.getValue.get(2, TimeUnit.SECONDS)}")
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