package main

import scala.util.control.Breaks._

import java.util.concurrent.TimeUnit
import java.util.Properties
import java.time.Duration

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.state.Stores

object GDELTStream extends App {

  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  //Our 2 KeyValue Stores

  //Name,Count store
  builder.addStateStore(Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("last-hour-counts"),
    Serdes.String,
    Serdes.Long))

  //Timestamp,Name store
  builder.addStateStore(Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("timestamp-log"),
    Serdes.String,
    Serdes.String))

  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  records.mapValues(_.split("\t", -1)) // Split it with tabs
    .filter { (_, v) => v.length > 23}               // Keep the uncorrupted data
    .mapValues(k => k(23).replaceAll("[,0-9]", "").split(";", -1)) // List of names
    .flatMapValues(x => x) // Flatten the list
    .filter{ (_, v) => !v.trim.equals("")} // Take out all the empty names
    .transform(new HistogramTransformer, "last-hour-counts", "timestamp-log") // Do the transformation
    .to("gdelt-histogram") // Send the results to the gdelt-histogram topic

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

class HistogramTransformer extends Transformer[String, String, (String, Long)] {

  var context: ProcessorContext = _
  var lastHourCounts: KeyValueStore[String, Long] = _
  var timestampLog: KeyValueStore[String, String] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {

    this.context = context

    //Get the StateStores in this transformer
    this.lastHourCounts = this.context.getStateStore("last-hour-counts").asInstanceOf[KeyValueStore[String, Long]]

    this.timestampLog = this.context.getStateStore("timestamp-log").asInstanceOf[KeyValueStore[String, String]]

    //The decrement process
    this.context.schedule(Duration.ofMillis(500).toMillis, PunctuationType.STREAM_TIME, timestamp => {

      val iterator: KeyValueIterator[String, String] = timestampLog.all()

      while (iterator.hasNext) {

        val e: KeyValue[String, String] = iterator.next()

        // If it was more than an hour has passed
        if (((timestamp - decodeTimestamp(e.key)) / 3.6e6) > 1) {
          //3. Decrement the count of a record an hour later.
          lastHourCounts.put(e.value, lastHourCounts.get(e.value) - 1L)
          //Delete it from the log
          timestampLog.delete(e.key)
        }
        //Output of a name with decremented count based on time
        context.forward(e.value, lastHourCounts.get(e.value))
      }
      context.commit()
    })
  }

  def transform(key: String, name: String): (String, Long) = {

    addLog(this.context.timestamp(), name)

    val nameCount: Long = lastHourCounts.get(name)

    if (nameCount == null.asInstanceOf[Long]) {
      //1. Add a new record to the histogram and initialize its count
      lastHourCounts.put(name, 1L)
    } else {
      //2. Change the count for a record if it occurs again
      lastHourCounts.put(name, nameCount + 1L)
    }

    //Output of a name with a new count
    (name, lastHourCounts.get(name))
  }

  def close(): Unit = {}

  // Add a log with a unique timestamp as a key and the name as a value
  def addLog(t: Long, name: String): Unit ={

    var i: Int = 1

    breakable {
      while (true) {

        //Create the unique timestamp
        val log: String = timestampLog.get(t+"-"+i)

        // If it does not exist add it or try again
        if (log == null.asInstanceOf[String]) {
          timestampLog.put(t+"-"+i, name)
          break
        }
        else {
          i += 1
        }
      }
    }
  }

  //A simple function to decode our timestamp
  def decodeTimestamp(s: String): Long ={
    s.split("-",-1)(0).toLong
  }

}
