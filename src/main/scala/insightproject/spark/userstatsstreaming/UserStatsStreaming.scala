package insightproject.spark.userstatsstreaming

/**
  * Created by rfrigato on 6/22/17.
  */
import java.time.LocalTime
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.{PreferConsistent, PreferBrokers}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.elasticsearch.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import scala.collection.JavaConversions._
import java.io._

/**
  * Creates a Kafka Direct Stream to read user statistics into Spark Streaming and send them to Elasticsearch
  */
object UserStatsStreaming {
  /**
    * Creates the Avro schema used to create the Avro record
    */
  val gdeltAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/user-stats-avro-schema.json")
    parser.parse(schemaFile)
  }

  /**
    * Converts an Avro record into Bytes
    */
  val recordInjection : Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(gdeltAvroSchema)

  /**
    * Creates a Spark Streaming Context
    *
    * @param topic the topic we are reading from
    * @param groupId the Consumer group id the context is using
    * @param checkpointDirectory the folder were the checkpoint is stored
    * @return
    */
  def getContext(topic: String, groupId: String, checkpointDirectory: String): StreamingContext = {
    val bootstrapServers: String = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers, // Kafka bootstrap servers
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("user_stats")
    val esNodes: String = getEnvOrThrow("ES_NODES")
    sparkConf.set("es.nodes", esNodes) // the elasticsearch nodes
    sparkConf.set("es.index.auto.create", "false") // do NOT create an index when it doesn't exist
    sparkConf.set("es.batch.write.refresh", "false") // do NOT refresh on write
    sparkConf.set("es.batch.size.entries", "1000") // size of the batch sent to elasticsearch
    sparkConf.set("es.net.http.auth.user", sys.env("ELASTIC_USER")) // elasticsearch user
    sparkConf.set("es.net.http.auth.pass", sys.env("ELASTIC_PASS")) // elasticsearch password
    sparkConf.set("spark.streaming.backpressure.enabled", "true") // enable backpressure
    val timeWindow = sys.env.getOrElse("TIME_WINDOW", "5").toInt
    val ssc = new StreamingContext(sparkConf, Seconds(timeWindow))
    ssc.checkpoint(checkpointDirectory)
    val topics = Array(topic)
    // Loads the location strategy from the ENV variable
    val locationStrategy = sys.env.get("LOCATION_STRATEGY") match {
      case Some("PreferBrokers") => PreferBrokers
      case _ => PreferConsistent
    }
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      locationStrategy,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )
    stream.foreachRDD { rdd =>
      rdd.map(record => {
        val bytes = record.value()
        convertAvroBytes(bytes)
      }).saveToEs("users/stats")
    }
    ssc
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new Exception("Missing parameter: topic and group id are required")
    }
    val topic = args(0)
    val groupId = args(1)
    val checkpointDirectory = getEnvOrThrow("SPARK_CHECKPOINT_FOLDER_STATS")
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => getContext(topic, groupId, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * Gets the variable or throws an exception
    *
    * @param envVariable
    * @return the value of the environment variable
    */
  private def getEnvOrThrow(envVariable: String): String = {
    sys.env.get(envVariable) match {
      case Some(value) => value
      case None => throw new Exception(s"Environment variable $envVariable not set")
    }
  }
  /**
    * Parses a binary Avro message containing user statistics into a Map
    *
    * @param bytes
    * @return the Map resulting from the Avro message
    */
  private def convertAvroBytes(bytes:Array[Byte]) = {
    val record = recordInjection.invert(bytes).get
    Map(
      "user_id" -> record.get("user_id").asInstanceOf[Int],
      "score_type" -> record.get("score_type").asInstanceOf[org.apache.avro.util.Utf8].toString,
      "num_user_topics" -> record.get("num_user_topics").asInstanceOf[Int],
      "num_new_user_topics" -> record.get("num_new_user_topics").asInstanceOf[Int],
      "news_id" -> record.get("news_id").asInstanceOf[org.apache.avro.util.Utf8].toString,
      "news_url" -> record.get("news_url").asInstanceOf[org.apache.avro.util.Utf8].toString,
      "timestamp" -> record.get("timestamp").asInstanceOf[Long],
      "took" -> record.get("took").asInstanceOf[Int]
    )
  }
}
