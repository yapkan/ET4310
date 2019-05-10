package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}


object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // create persistent KeyValueStore
  val countStoreSupplier = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("Counts"), Serdes.String, Serdes.Long)
    .withLoggingDisabled()
  val countStore = countStoreSupplier.build

  // add the KeyValueStore to the StreamsBuilder
  val builder: StreamsBuilder = new StreamsBuilder
  builder.addStateStore(countStoreSupplier)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram. 
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  // flatten the allNames column
  val allNames = records
    .mapValues((key, value) => value.split("\t"))
    .filter((key, value) => value.length > 23)
    .flatMapValues((key, value) => value(23).split(",[0-9;]+").filter(_ != ""))

  // apply HistogramTransformer and publish result to gdelt-histogram topic
  val histogramTransformer = new HistogramTransformer
  val transformed = allNames.transform(histogramTransformer, "Counts").to("gdelt-histogram")

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

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var kvStore: KeyValueStore[String, Long] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context

    // retrieve the KeyValueStore named "Counts"
    this.kvStore = context.getStateStore("Counts").asInstanceOf[KeyValueStore[String, Long]]
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {

    // calculate interval for the decrement schedule
    val recordTimestamp: Long = context.timestamp()
    val currentTimestamp: Long = System.currentTimeMillis()
    val retentionPeriod: Long = TimeUnit.HOURS.toMillis(1)
    val interval = recordTimestamp - currentTimestamp + retentionPeriod

    // schedule decrement of the count after an hour
    var scheduled: Cancellable = null
    scheduled = context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, (timestamp) => {

      // decrement by 1
      val currentCount = kvStore.get(name)
      val newCount = currentCount - 1L
      kvStore.put(name, newCount)
      context.forward(name, newCount)

      // cancel this schedule, so it runs only once
      scheduled.cancel()
    })

    // add new record to StateStore
    kvStore.putIfAbsent(name, 0L)

    // increment count by 1
    val currentCount = kvStore.get(name)
    val newCount = currentCount + 1L
    kvStore.put(name, newCount)

    // return current count of the name during the last hour
    (name, newCount)
  }

  // Close any resources if any
  def close() {
  }
}
