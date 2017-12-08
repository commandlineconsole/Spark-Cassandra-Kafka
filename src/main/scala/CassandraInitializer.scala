import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.cql.CassandraConnector

object CassandraInitializer {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("Load All Information to Cassandra")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "10.1.51.42")
//      .set("spark.cassandra.connection.host", args(0).toString)
    val sc = new SparkContext(conf)

    val cassandraConnector = CassandraConnector.apply(conf)

    cassandraConnector.withSessionDo(session => {
      session.execute(s"""DROP KEYSPACE IF EXISTS datawatch;""")
      session.execute("CREATE KEYSPACE IF NOT EXISTS datawatch WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} ")
      session.execute(s"""CREATE TABLE IF NOT EXISTS datawatch.bidoffertrade_from_consumer (
						sym text,
						isodatetime timestamp,
						ask_volume int,
						ask_price float,
						bid_volume int,
						bid_price float,
						trade_price float,
						trade_volume int,
						PRIMARY KEY (sym, isodatetime, ask_volume, ask_price, bid_volume, bid_price)
					) WITH CLUSTERING ORDER BY (isodatetime ASC, ask_volume ASC, ask_price ASC, bid_volume ASC, bid_price ASC)
					;""")
      session.execute(s"""CREATE TABLE IF NOT EXISTS datawatch.bidoffertrade_from_consumer_sec (
            sym text,
						dt_truncated timestamp,
						ask_volume int,
						ask_price float,
						bid_volume int,
						bid_price float,
						trade_price float,
						sum_trade_volume bigint,
						PRIMARY KEY (sym, dt_truncated)
					) 
					;""")
      session.execute(s"""CREATE TABLE IF NOT EXISTS datawatch.bidoffertrade_from_consumer_min (
            sym text,
						dt_truncated_Minutes timestamp,
						ask_volume int,
						ask_price float,
						bid_volume int,
						bid_price float,
						trade_price float,
						sum_trade_volume bigint,
						PRIMARY KEY (sym, dt_truncated_Minutes)
					) 
					;""")
      session.execute(s"""TRUNCATE TABLE datawatch.bidoffertrade_from_consumer;""")
      session.execute(s"""TRUNCATE TABLE datawatch.bidoffertrade_from_consumer_sec;""")
      session.execute(s"""TRUNCATE TABLE datawatch.bidoffertrade_from_consumer_min;""")

      
    })
    System.out.println("==================================================================================")
    System.out.println("Created Keyspace 'datawatch' with tables 'bidoffertrade_from_consumer' 'bidoffertrade_from_consumer_sec' 'bidoffertrade_from_consumer_min' ")
    System.out.println("Truncated tables 'bidoffertrade_from_consumer' 'bidoffertrade_from_consumer_sec' 'bidoffertrade_from_consumer_min' ")
    System.out.println("==================================================================================")

    sc.stop()
  }
}