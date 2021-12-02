package example

import scala.collection.JavaConverters._
import scala.util.control._
import scala.util.control.Breaks._
import scala.util.{Try, Success, Failure}
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
//import com.typesafe.config.ConfigFactory
//import configs.ConfigReader
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

/*Apparently pureconfig is better than typesafe conf, but the docs recommend against using it with Scala < 2.12, and we're using 2.11
import pureconfig._
import pureconfig.generic.auto._

case class Port(number: Int) extends AnyVal

case class Conf(
  host: String,
  port: Port
)
*/

object Main {
  //import org.apache.kafka.streams.scala.ImplicitConversions._
  //import org.apache.kafka.streams.scala.Serdes._

  val CREATE_TOPIC = 1
  val PRODUCE_MESSAGE = 2
  val CONSUME_MESSAGES = 3
  val CREATE_PROCESSOR = 4
  val DELETE_PROCESSOR = 5
  val DELETE_TOPIC = 6
  val DISPLAY_TOPICS = 7
  val EXIT = 8

	val rootLogger = Logger.getRootLogger()
	rootLogger.setLevel(Level.ERROR)
	
	Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
	Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  //implicit val messageSerde = new StreamsSerde().messageSerde

  /*
  val props = new Properties()
  props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString("bootstrap.servers"))
  props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, conf.getString("transactions.application.id"))

  val builder = new StreamsBuilder
  val transactionsKStream = builder.stream[String, Transaction](conf.getString("transactions.application.source.topic")).mapValues(t => t.foo)

  transactionsKStream.to(conf.gettring("transactions.application.sink.topic"))

  val streams = new KafkaStreams(builder.build(), props)

  streams.start

  sys.ShutdownHookThread {
    streams.close(10, TimeUnit.SECONDS)
  }

  sys.allThreads.foreach(println)
  */

  var adminClient: Option[AdminClient] = None
  var conf: Option[Config] = None

  def main(args: Array[String]): Unit = {
    Try {
      init()
    } match {
      case Success(_) => {
        println("Successfully initialized Kafka connection\n")

        var loop = true
        while(loop) {
          println("Main Menu")
          println("---------")
          println("1. Create a topic")
          println("2. Send a message about a topic")
          println("3. Retrieve all messages for a topic")
          println("4. Start processing events as a stream")
          println("5. Stop stream processing")
          println("6. Delete a topic")
          println("7. Show all topics")
          println("8. Exit")
          print("\nOption: ")

          Try(readInt) match {
            case Success(option) => option match {
              case CREATE_TOPIC => createTopic(readLine("Topic name? "))
              case PRODUCE_MESSAGE => produceMessageForTopic(readLine("Topic name? "), readLine("Message? "))
              case CONSUME_MESSAGES => consumeMessagesForTopic(readLine("Topic name? "))
              case CREATE_PROCESSOR => createProcessor(readLine("Topic name? "), println)
              case DELETE_PROCESSOR => deleteProcessor("Topic name? ")
              case DELETE_TOPIC => deleteTopic(readLine("Topic name? "))
              case DISPLAY_TOPICS => displayAllTopics
              case EXIT => loop = false
              case n => println(s"\nError: $n is not an option\n")
            }
            case Failure(exception) => {
              println(s"\nError (${exception.getClass}): ${exception.getMessage}\n")
            }
          }
        }

        cleanup()
        println("Finished closing Kafka connection and freeing resources\n")
      }

      case Failure(exception) => println(s"$exception\n")
    }
  }

  def init(): Boolean = {
    var result = false

    if(conf.isEmpty) {
      ConfigFactory.invalidateCaches
      conf = Some(ConfigFactory.load)
    }

    if(adminClient.isEmpty) {
      val props: Properties = new Properties()

      props.put("bootstrap.servers", s"${conf.get.getString("kafkaDemo.kafka.host")}:${conf.get.getString("kafkaDemo.kafka.port")}")
      /*
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("acks", "all")
      */

      adminClient = Some(AdminClient.create(props))
      println("Connected to Kafka server as administrator")

      result = true
    }

    result
  }
  
  def cleanup(): Boolean = {
    if(adminClient.isDefined) {
      adminClient.get.close()
    }
    
    true
  }

  def listTopics(): java.util.Set[String] = {
    if(adminClient.isEmpty) {
      throw new Exception("Unable to connect to Kafka")
    }
    
    adminClient.get.listTopics.names.get
  }

  def topicExists(topic: String): Boolean = {
    listTopics.contains(topic)
  }

  def createTopic(topic: String, partitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    var result = false

    if(adminClient.isDefined) {
      val createTopicsResult = adminClient.get.createTopics(java.util.Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))

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
    }

    result
  }

  def produceMessageForTopic(topic: String, message: String): Boolean = {
    var result = false

    if(adminClient.isDefined) {
      val props: Properties = new Properties()
      props.put("group.id", "default")
      props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      props.put(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      props.put(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )

      // The acks config controls the criteria under which requests are considered complete.
      // The "all" setting we have specified will result in blocking on the full commit of the record,
      // the slowest but most durable setting.
      props.put("acks", "all")

      val producer = new KafkaProducer[String, String](props)

      Try {
        //val key = message(0).toInt % 10
        val key = 1
        val record = new ProducerRecord[String, String](topic, key.toString, message)
        val metadata = producer.send(record)
        println(s"Sent record { key = '$key', value = '$message' }\nReceived metadata { partition = '${metadata.get.partition}', offset = '${metadata.get.offset}' }")
      } match {
        case Success(x) => println(s"Success: $x")
        case Failure(e) => println(s"Failure: $e")
      }

      producer.close()
      result = true
    }

    result
  }

  def consumeMessagesForTopic(topic: String): Boolean = {
    var result = false

    if(adminClient.isDefined) {
      val props: Properties = new Properties()
      props.put("group.id", "default")
      // props.put("bootstrap.servers","localhost:9092")
      props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      props.put(
        "key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer"
      )
      props.put(
        "value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer"
      )
      props.put("enable.auto.commit", "true")
      props.put("auto.commit.interval.ms", "1000")

      val consumer = new KafkaConsumer(props)
      val topics = List(topic)
      var recordCount = 0
      var time = 0

      Try {
        consumer.subscribe(topics.asJava)

        val partitions = consumer.assignment
        var offsets = consumer.beginningOffsets(partitions)
        var it = partitions.iterator

        while(it.hasNext) {
          val i = it.next
          consumer.seek(i, offsets.get(i))
        }

        val timeout = 2000

        println(s"Polling for records on topic '$topic'...")
        while(time < timeout) {
          val records = consumer.poll(10)
          time += 10

          for(record <- records.asScala) {
            recordCount += 1

            println(s"Retrieved record #$recordCount: (key = ${record.key}, value = ${record.value}, offset = ${record.offset}, partition = ${record.partition})")
          }
        }

        println(s"Retrieved $recordCount records about topic $topic in ${time/1000} seconds")
      } match {
        case Success(_) => {}
        case Failure(e) => println(e)
      }

      consumer.close()
      result = true
    }
    
    result
  }

  def processAsStream(topic: String) {
    /*
    val props = new Properties
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get.getString("bootstrap.servers"))
    //props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, conf.getString("transactions.application.id"))

    val builder = new StreamsBuilder

    val messageStream = builder.stream[String, String](topic, Consumed.with(Serdes.String(), Serdes.String())).mapValues(x => println(s"Event occurred for topic '$x'"))

    val streams = new KafkaStreams(builder.build, props)

    streams.start

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }
    */

    /*
    import org.apache.kafka.common.serialization.Serdes
    import org.apache.kafka.streams.StreamsBuilder
    import org.apache.kafka.streams.kstream.KStream
    import org.apache.kafka.streams.kstream.Consumed

    var builder = new StreamsBuilder
    var stream: KStream[String, String] = builder.stream(topic, Consumed.with(Serdes.String, Serdes.String))
    */

    import java.time.Duration
    import java.util.Properties

    import org.apache.kafka.streams.kstream.Materialized
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala._
    import org.apache.kafka.streams.scala.kstream._
    import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

    import Serdes._
    
    val props = new Properties
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get.getString("bootstrap.servers"))

    val builder = new StreamsBuilder
    val stream = builder.stream[String, String](topic).peek((key, values) => println(s"$key: $values"))

    /*
    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count(Materialized.as("counts-store"))
    wordCounts.toStream.to("WordsWithCountsTopic")
    */

    val streams: KafkaStreams = new KafkaStreams(builder.build, props)
    streams.start

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

  def createProcessor(topic: String, fn: Function[Unit, Unit]): Boolean = {
    var result = false

    if(adminClient.isDefined) {

    }

    result
  }

  def deleteProcessor(topic: String): Boolean = {
    var result = false

    if(adminClient.isDefined) {

    }

    result
  }

  def deleteTopic(topic: String): Boolean = {
    var result = false

    if(adminClient.isDefined) {      
      topicExists(topic) match {
        case true => {
          val deleteTopicsResult = adminClient.get.deleteTopics(java.util.Collections.singletonList(topic))

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
        }

        case false => println(s"Topic '$topic' not found")
      }
    }

    result
  }

  def displayAllTopics(): Boolean = {
    println(listTopics)

    true
  }
}