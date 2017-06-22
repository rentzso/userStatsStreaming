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

object UserStatsStreaming {
  val gdeltAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/user-stats-avro-schema.json")
    parser.parse(schemaFile)
  }
  val logFile = "/tmp/user-stats.log"
  val bw = new BufferedWriter(new FileWriter(logFile))

  val recordInjection : Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(gdeltAvroSchema)
  def getContext(topic: String, groupId: String, checkpointDirectory: String): StreamingContext = {
    val bootstrapServers: String = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("user_stats")
    val esNodes: String = getEnvOrThrow("ES_NODES")
    sparkConf.set("es.nodes", esNodes)
    sparkConf.set("es.index.auto.create", "false")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.batch.size.entries", "1000")
    sparkConf.set("es.net.http.auth.user", sys.env("ELASTIC_USER"))
    sparkConf.set("es.net.http.auth.pass", sys.env("ELASTIC_PASS"))
    val timeWindow = sys.env.getOrElse("TIME_WINDOW", "10").toInt
    val ssc = new StreamingContext(sparkConf, Seconds(timeWindow))
    ssc.checkpoint(checkpointDirectory)
    val topics = Array(topic)
    val locationStrategy = sys.env.get("LOCATION_STRATEGY") match {
      case Some("PreferBrokers") => PreferBrokers
      case _ => PreferConsistent
    }
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      locationStrategy,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )

    def sumReduce(a: (Int, Int), b:(Int, Int)) = {
      (a._1 + b._1, a._2 + b._2)
    }
    def inverseReduce(a: (Int, Int), b:(Int, Int)) = {
      (a._1 - b._1, a._2 - b._2)
    }

    val results = stream.map(record => (record.key, record.value)).mapValues(
      msg => {
        val avroMsg = convertAvroBytes(msg)
        (avroMsg("num_user_topics").asInstanceOf[Int], 1)
      }
    ).reduceByKeyAndWindow(
      sumReduce(_, _), inverseReduce(_, _), Seconds(timeWindow), Seconds(3 * timeWindow.toInt)
    )
    results.foreachRDD( rdd =>
      rdd.mapValues((sumAndCountTuple: (Int, Int)) => {
        if (sumAndCountTuple._2 == 0) {
          0
        } else {
          // computes the average
          sumAndCountTuple._1.toDouble/sumAndCountTuple._2
        }
      }).map({
        case (scoreType, avg) => {
          Map(
            "score_type" -> scoreType,
            "average" -> avg,
            "timestamp" -> System.currentTimeMillis / 1000
          )
        }
      }).saveToEs("users/stats")
    )
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
  private def getEnvOrThrow(envVariable: String): String = {
    sys.env.get(envVariable) match {
      case Some(value) => value
      case None => throw new Exception(s"Environment variable $envVariable not set")
    }
  }
  private def convertAvroBytes(bytes:Array[Byte]) = {
    val record = recordInjection.invert(bytes).get
    Map(
      "user_id" -> record.get("user_id").asInstanceOf[Int],
      "score_type" -> record.get("score_type").asInstanceOf[org.apache.avro.util.Utf8].toString,
      "num_user_topics" -> record.get("num_user_topics").asInstanceOf[Int]
    )
  }

}
