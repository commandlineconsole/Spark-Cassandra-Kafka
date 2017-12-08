import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{date_format, last, max, row_number, sum}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.{JSONArray, JSONObject}

//bin/spark-shell --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1,com.datastax.spark:spark-cassandra-connector_2.10:1.6.0 --conf spark.cassandra.connection.host=10.1.51.39
object Consumer extends App {

  case class bidoffer(sym: String, isodatetime: String, ask_price: Double, ask_volume: Int, bid_price: Double, bid_volume: Int, trade_price: Double, trade_volume: Int)

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val appName = "SparkKafkaConsumer"

  val conf = new SparkConf()
    //.set("spark.cassandra.connection.host", "10.1.51.39")
    .set("spark.cassandra.connection.host", args(0).toString)
    .set("spark.cores.max", "2")
    .set("spark.executor.memory", "1g")
    .set("spark.rdd.compress", "true")
    .set("spark.storage.memoryFraction", "1")
    .set("spark.streaming.unpersist", "true")
    .set("spark.sql.thriftserver.scheduler.pool","accounting")
    .set("spark.sql.shuffle.partitions","10")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName(appName)
    .setMaster("local")

  val sc = SparkContext.getOrCreate(conf)
  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  import sqlContext.implicits._

  val ssc = new StreamingContext(sc, Seconds(20))


  //val kafkaTopics = Set("test")
  val kafkaTopics = Set(args(1).toString)
  //val kafkaParams = Map[String, String]("metadata.broker.list" -> "10.1.51.39:9092") //"zookeeper.connect" -> "10.1.51.39:2181")
  val kafkaParams = Map[String, String]("metadata.broker.list" -> args(2).toString) //"zookeeper.connect" -> "10.1.51.39:2181")

  //Producer1
  val producderProps = new java.util.Properties()
  //producderProps.put("metadata.broker.list", "10.1.51.39:9092") // dev
  producderProps.put("metadata.broker.list", args(2).toString) // dev
  //producderProps.put("zookeeper.connect", "10.1.51.39:2181");
  producderProps.put("producer.type", "sync")
  producderProps.put("compression.codec", "2")
  producderProps.put("serializer.class", "kafka.serializer.StringEncoder")
  producderProps.put("client.id", "camus")

  val producerConfig = new ProducerConfig(producderProps)
  val producer = new Producer[String, String](producerConfig)
  //producer1END

  //Producer2
  val producder2Props = new java.util.Properties()
  //producder2Props.put("metadata.broker.list", "10.1.51.39:9093") // dev
  producder2Props.put("metadata.broker.list", args(3).toString) // dev
  //producderProps.put("zookeeper.connect", "10.1.51.39:2181");
  producder2Props.put("producer.type", "sync")
  producder2Props.put("compression.codec", "2")
  producder2Props.put("serializer.class", "kafka.serializer.StringEncoder")
  producder2Props.put("client.id", "camus")

  val producer2Config = new ProducerConfig(producder2Props)
  val producer2 = new Producer[String, String](producer2Config)
  //producer2END

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

  kafkaStream.foreachRDD {
    (message: RDD[(String, String)], batchTime: Time) => {
      val bidoffertrade_rdd = message.map {
        case (k, v) => v.split(",")
      }.map(payload => {
          //val isodatetime = Timestamp.valueOf(payload(2))
          //val trade_price =
          bidoffer(payload(0), payload(1), payload(2).toDouble, payload(3).toInt, payload(4).toDouble,payload(5).toInt, payload(6).toDouble, payload(7).toInt)
        }).toDF("sym", "isodatetime", "ask_price", "ask_volume", "bid_price", "bid_volume", "trade_price" , "trade_volume").cache()
      bidoffertrade_rdd.select("sym", "isodatetime", "ask_volume", "ask_price", "bid_price", "bid_volume", "trade_price" , "trade_volume").write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "datawatch", "table" -> "bidoffertrade_from_consumer")).save()
      //bidoffertrade_rdd.show(5)
      // declare the rdd name as bidoffertrade_rdd
      bidoffertrade_rdd.show(false)
      bidoffertrade_rdd.count()
      val datedf = date_format(bidoffertrade_rdd("isodatetime"), "yyyy-MM-dd HH:mm:ss")
      val time=bidoffertrade_rdd.withColumn("dt_truncated",datedf)
      val overCategory = Window.partitionBy('dt_truncated ).orderBy('isodatetime.asc)
      val row_num = row_number.over(overCategory)
      val ranked = time.withColumn("row_num", row_number.over(overCategory))
      val df_tv=ranked.groupBy("sym","dt_truncated").agg(sum("trade_volume").alias("sum_trade_volume")).cache()
      // val df_tpNN = ranked.select("sym","dt_truncated","trade_price","row_num").filter(ranked("trade_price").isNotNull)
      val df_tpNN = ranked.select("sym","dt_truncated","trade_price","row_num").filter(ranked("trade_price") !== 0)
      val df_tpMax=df_tpNN.groupBy("sym","dt_truncated").agg(max("row_num").alias("row_num"))
      val df_tp = df_tpNN.join(df_tpMax, df_tpNN("sym")===df_tpMax("sym") && df_tpNN("dt_truncated")===df_tpMax("dt_truncated") && df_tpNN("row_num")===df_tpMax("row_num")).select(df_tpNN("sym"), df_tpNN("trade_price"), df_tpNN("dt_truncated"), df_tpNN("row_num"))
      val df_restMax=ranked.groupBy("sym","dt_truncated").agg(max("row_num").alias("row_num"))
      val df_rest = ranked.join(df_restMax, ranked("sym")===df_restMax("sym") && ranked("dt_truncated")===df_restMax("dt_truncated") && ranked("row_num")===df_restMax("row_num")).select(ranked("sym"), ranked("isodatetime"), ranked("ask_price"), ranked("ask_volume"), ranked("bid_price"), ranked("bid_volume"), ranked("dt_truncated"), ranked("row_num"))
      val df_sfinal = df_tv.join(df_tp, df_tv("sym")===df_tp("sym") && df_tv("dt_truncated")===df_tp("dt_truncated")).drop(df_tp("sym")).drop(df_tp("dt_truncated"))
      val df_rfinal = df_sfinal.toDF("sum_trade_volume", "symbol", "trade_price", "dt_truncated_date", "row_number")
      val df_final = df_rfinal.join(df_rest, df_rfinal("symbol")===df_rest("sym") && df_rfinal("dt_truncated_date")===df_rest("dt_truncated")).drop(df_rfinal("symbol")).drop(df_rfinal("dt_truncated_date")).drop(df_rfinal("row_number")).select("sym","dt_truncated","ask_volume","ask_price","bid_volume","bid_price","trade_price","sum_trade_volume").cache()
      df_final.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "datawatch", "table" -> "bidoffertrade_from_consumer_sec")).save()
      df_final.show(false)
      val df_minutes = df_final.cache()
      val dateMdf = date_format(df_minutes("dt_truncated"),"yyyy-MM-dd HH:mm")
      val df_finalMinutes = df_final.withColumn("dt_truncated_Minutes",dateMdf)
      val df_mindrop = df_finalMinutes.drop("dt_truncated")
      val df_final2 = df_mindrop.groupBy("sym","dt_truncated_Minutes").agg(sum("sum_trade_volume").alias("sum_trade_volume"),last("trade_price").alias("trade_price"),last("ask_price").alias("ask_price"),last("ask_volume").alias("ask_volume"),last("bid_price").alias("bid_price"),last("bid_volume").alias("bid_volume")).select("sym","dt_truncated_minutes","ask_volume","ask_price","bid_volume","bid_price","trade_price","sum_trade_volume").cache()
      df_final2.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "datawatch", "table" -> "bidoffertrade_from_consumer_min")).save()
      df_final2.show(false)
      //println(s"${df.count()} rows processed.")
      //producer 1
      //val stateJO = new JSONObject()
      val mainJSONSecond = new JSONArray()
      df_final.collect().foreach(r => {
        //val stateDetails = new JSONObject()
        val secondDetails = new JSONObject()
        //secondDetails = r.get(0) //"sym","dt_truncated","ask_volume","ask_price","bid_volume","bid_price","trade_price","sum_trade_volume"
        secondDetails.put("sym", r.get(0))
        secondDetails.put("dt_truncated", r.get(1))
        secondDetails.put("ask_volume", r.get(2))
        secondDetails.put("ask_price", r.get(3))
        secondDetails.put("bid_volume", r.get(4))
        secondDetails.put("bid_price", r.get(5))
        secondDetails.put("trade_price", r.get(6))
        secondDetails.put("trade_volume_sum", r.get(7))
        mainJSONSecond.put(secondDetails)
      })

      println("Result JSON: " + mainJSONSecond.toString())
        // send the data to kafka topic "seconds"
       // val data1 = new KeyedMessage[String, String]("seconds", mainJSONSecond.toString())
        //        println("Produced message ===>" + data.toString())
       // producer.send(data1)
      //mainJSONSecond.put("",secondDetails)
      val data = new KeyedMessage[String, String]("seconds", mainJSONSecond.toString())
      // //        println("Produced message ===>" + data.toString())
      producer.send(data)
      //producer 2
      //val stateJO = new JSONObject()
      val mainJSONMinute = new JSONArray()
      df_final2.collect().foreach(r => {
        //val stateDetails = new JSONObject() //("sym","dt_truncated_minutes","ask_volume","ask_price","bid_volume","bid_price","trade_price","sum_trade_volume")
        val minuteDetails = new JSONObject()
        minuteDetails.put("sym", r.get(0))
        minuteDetails.put("dt_truncated_minutes", r.get(1))
        minuteDetails.put("ask_volume", r.get(2))
        minuteDetails.put("ask_price", r.get(3))
        minuteDetails.put("bid_volume", r.get(4))
        minuteDetails.put("bid_price", r.get(5))
        minuteDetails.put("trade_price", r.get(6))
        minuteDetails.put("sum_trade_volume", r.get(7))
        mainJSONMinute.put(minuteDetails)
      })
      println("Result JSON: " + mainJSONMinute.toString())
        // send the data to kafka topic "minutes"
       //val data2 = new KeyedMessage[String, String]("minutes", mainJSONMinute.toString())
        //        println("Produced message ===>" + data.toString())
       // producer.send(data2)
      // mainJSONMinute.put("",minuteDetails)
      val data1 = new KeyedMessage[String, String]("minutes", mainJSONMinute.toString())
      //        println("Produced message ===>" + data.toString())
      producer2.send(data1)
    }
  }

  ssc.start()
  ssc.awaitTermination()
}