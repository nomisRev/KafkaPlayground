package io.github.nomisrev.kafka

import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.deleteTopic
import io.github.nomisRev.kafka.describeTopic
import io.github.nomisRev.kafka.publisher.Acks
import io.github.nomisRev.kafka.publisher.KafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.publisher.TransactionalScope
import io.github.nomisRev.kafka.receiver.AutoOffsetReset
import io.github.nomisRev.kafka.receiver.KafkaReceiver
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

private val testIterations: Int =
  System.getProperties().getProperty("io.github.nomisrev.kafka.TEST_ITERATIONS")?.toIntOrNull() ?: 1

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class KafkaSpec {

  companion object {
    private val consumerPollingTimeout = 1.seconds
    private val transactionTimeoutInterval = 1.seconds

    @AfterAll
    @JvmStatic
    fun destroy() {
      kafka.stop()
    }

    @BeforeAll
    @JvmStatic
    fun setup() {
      kafka.start()
    }

    @JvmStatic
    val kafka: Kafka =
      Kafka().apply {
        withExposedPorts(9092, 9093)
        withNetworkAliases("broker")
        withEnv("KAFKA_HOST_NAME", "broker")
        withEnv("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR", "1")
        withEnv("KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR", "1")
        withEnv(
          "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
          transactionTimeoutInterval.inWholeMilliseconds.toString()
        )
        withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
        withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
        withReuse(true)
      }

    fun KafkaReceiver(): KafkaReceiver<String, String> =
      KafkaReceiver(receiverSetting())

    fun receiverSetting(): ReceiverSettings<String, String> =
      ReceiverSettings(
        bootstrapServers = kafka.bootstrapServers,
        keyDeserializer = StringDeserializer(),
        valueDeserializer = StringDeserializer(),
        groupId = "test-group-id",
        autoOffsetReset = AutoOffsetReset.Earliest,
        pollTimeout = consumerPollingTimeout
      )

    fun publisherSettings() = PublisherSettings(
      bootstrapServers = kafka.bootstrapServers,
      keySerializer = StringSerializer(),
      valueSerializer = StringSerializer(),
      properties = Properties().apply {
        put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000.toString())
        put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000.toString())
      }
    )

    suspend fun <A> publishScope(block: suspend TransactionalScope<String, String>.() -> A): A =
      KafkaPublisher(publisherSettings()).use { it.publishScope(block) }
  }

  private fun adminProperties(): Properties = Properties().apply {
    put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
    put(AdminClientConfig.CLIENT_ID_CONFIG, "test-kafka-admin-client-${UUID.randomUUID()}")
    put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000")
  }

  fun adminSettings(): AdminSettings =
    AdminSettings(kafka.bootstrapServers, adminProperties())

  inline fun <A> admin(body: Admin.() -> A): A =
    Admin(adminSettings()).use(body)

  fun publisherSettings(
    acknowledgments: Acks = Acks.One,
    properties: Properties.() -> Unit
  ): PublisherSettings<String, String> {
    val publisherSettings = publisherSettings()
    return publisherSettings().copy(
      acknowledgments = acknowledgments,
      properties = Properties().apply {
        properties()
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, publisherSettings.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, publisherSettings.keySerializer::class.qualifiedName)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, publisherSettings.valueSerializer::class.qualifiedName)
        put(ProducerConfig.ACKS_CONFIG, acknowledgments.value)
      }
    )
  }

  //<editor-fold desc="utilities">
  private fun nextTopicName(): String =
    "topic-${UUID.randomUUID()}"

  class TopicTestScope(
    val topic: NewTopic,
    scope: CoroutineScope
  ) : CoroutineScope by scope {
    fun produce(count: Int): List<ProducerRecord<String, String>> =
      produce(0 until count)

    fun produce(range: IntRange): List<ProducerRecord<String, String>> =
      range.map { createProducerRecord(it) }

    fun createProducerRecord(index: Int, partitions: Int = 4): ProducerRecord<String, String> {
      val partition: Int = index % partitions
      return ProducerRecord<String, String>(topic.name(), partition, "$index", "Message $index")
    }
  }

  fun withTopic(
    topicConfig: Map<String, String> = emptyMap(),
    partitions: Int = 4,
    replicationFactor: Short = 1,
    test: suspend TopicTestScope.(NewTopic) -> Unit
  ): Unit = repeat(testIterations) {
    runTest {
      val topic = NewTopic(nextTopicName(), partitions, replicationFactor).configs(topicConfig)
      admin {
        createTopic(topic)
        try {
          TopicTestScope(topic, this@runTest).test(topic)
        } finally {
          topic.shouldBeEmpty()
          deleteTopic(topic.name())
        }
      }
    }
  }

  object Boom : RuntimeException("Boom!") {
    private fun readResolve(): Any = Boom
  }

  @JvmName("publishPairsToKafka")
  suspend fun publishToKafka(
    topic: NewTopic,
    messages: Iterable<Pair<String, String>>,
  ): Unit =
    publishToKafka(messages.map { (key, value) ->
      ProducerRecord(topic.name(), key, value)
    })

  suspend fun publishToKafka(messages: Iterable<ProducerRecord<String, String>>): Unit =
    publishScope {
      offer(messages)
    }
  //</editor-fold>

  //<editor-fold desc="Assertions">
  suspend fun <K, V> KafkaReceiver<K, V>.committedCount(topic: String): Long =
    admin {
      val description = requireNotNull(describeTopic(topic)) { "Topic $topic not found" }
      val topicPartitions = description.partitions().map {
        TopicPartition(topic, it.partition())
      }.toSet()

      withConsumer {
        committed(topicPartitions)
          .mapNotNull { (_, offset) ->
            offset?.takeIf { it.offset() > 0 }?.offset()
          }.sum()
      }
    }

  suspend fun NewTopic.shouldBeEmpty() {
    val res = withTimeoutOrNull(100) {
      KafkaReceiver()
        .receive(name())
        .take(1)
        .toList()
    }
    if (res != null) throw AssertionError("Expected test to timeout, but found $res")
  }

  suspend infix fun NewTopic.assertHasRecord(records: ProducerRecord<String, String>) {
    assertEquals(
      KafkaReceiver()
        .receive(name())
        .map {
          it.apply { offset.acknowledge() }
        }.take(1)
        .map { it.value() }
        .toList(),
      listOf(records.value())
    )
    shouldBeEmpty()
  }

  suspend infix fun NewTopic.assertHasRecordCount(records: Int) {
    assertEquals(
      KafkaReceiver()
        .receive(name())
        .map { record -> record.offset.acknowledge() }
        .take(records)
        .count(),
      records
    )
    shouldBeEmpty()
  }

  suspend infix fun NewTopic.assertHasRecords(records: Iterable<ProducerRecord<String, String>>) {
    assertEquals(
      KafkaReceiver()
        .receive(name())
        .map { record ->
          record
            .also { record.offset.acknowledge() }
        }
        .take(records.toList().size)
        .toList()
        .groupBy({ it.partition() }) { it.value() },
      records.groupBy({ it.partition() }) { it.value() }
    )
    shouldBeEmpty()
  }

  @JvmName("shouldHaveAllRecords")
  suspend infix fun NewTopic.assertHasRecords(
    records: Iterable<Iterable<ProducerRecord<String, String>>>
  ) {
    val expected =
      records.flatten().groupBy({ it.partition() }) { it.value() }.mapValues { it.value.toSet() }
    assertEquals(
      KafkaReceiver()
        .receive(name())
        .map { record ->
          record.also { record.offset.acknowledge() }
        }
        .take(records.flatten().size)
        .toList()
        .groupBy({ it.partition() }) { it.value() }
        .mapValues { it.value.toSet() },
      expected
    )
    shouldBeEmpty()
  }

  //</editor-fold>
  //<editor-fold desc="Description">
  fun stubProducer(failOnNumber: Int? = null): suspend (PublisherSettings<String, String>) -> Producer<String, String> =
    {
      val producer = KafkaProducer(it.properties(), it.keySerializer, it.valueSerializer)
      object : Producer<String, String> {
        override fun clientInstanceId(p0: Duration?): Uuid =
          producer.clientInstanceId(p0)

        override fun close() {}

        override fun close(timeout: Duration?) {}

        override fun initTransactions() =
          producer.initTransactions()

        override fun beginTransaction() =
          producer.beginTransaction()

        @Suppress("OVERRIDE_DEPRECATION")
        override fun sendOffsetsToTransaction(
          offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
          consumerGroupId: String?
        ) = producer.sendOffsetsToTransaction(offsets, ConsumerGroupMetadata(consumerGroupId))

        override fun sendOffsetsToTransaction(
          offsets: MutableMap<TopicPartition, OffsetAndMetadata>?,
          groupMetadata: ConsumerGroupMetadata?
        ) = producer.sendOffsetsToTransaction(offsets, groupMetadata)

        override fun commitTransaction() =
          producer.commitTransaction()

        override fun abortTransaction() =
          producer.abortTransaction()

        override fun flush() =
          producer.flush()

        override fun partitionsFor(topic: String?): MutableList<PartitionInfo> =
          producer.partitionsFor(topic)

        override fun metrics(): MutableMap<MetricName, out Metric> =
          producer.metrics()

        override fun send(record: ProducerRecord<String, String>, callback: Callback): Future<RecordMetadata> =
          if (failOnNumber != null && record.key() == failOnNumber.toString()) {
            Executors.newScheduledThreadPool(1).schedule(
              {
                callback.onCompletion(null, Boom)
              },
              50,
              TimeUnit.MILLISECONDS
            )

            CompletableFuture.supplyAsync { throw AssertionError("Should never be called") }
          } else producer.send(record, callback)

        override fun send(record: ProducerRecord<String, String>): Future<RecordMetadata> =
          producer.send(record)
      }
    }
  //</editor-fold>
}
