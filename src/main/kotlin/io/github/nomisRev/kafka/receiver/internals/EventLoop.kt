package io.github.nomisRev.kafka.receiver.internals

import io.github.nomisRev.kafka.receiver.CommitStrategy
import io.github.nomisRev.kafka.receiver.Offset
import io.github.nomisRev.kafka.receiver.ReceiverSettings
import io.github.nomisRev.kafka.receiver.internals.AckMode.ATMOST_ONCE
import io.github.nomisRev.kafka.receiver.internals.AckMode.AUTO_ACK
import io.github.nomisRev.kafka.receiver.internals.AckMode.EXACTLY_ONCE
import io.github.nomisRev.kafka.receiver.internals.AckMode.MANUAL_ACK
import io.github.nomisRev.kafka.receiver.size
import java.time.Duration as JavaDuration
import java.time.Duration.ofSeconds
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.toJavaDuration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart.LAZY
import kotlinx.coroutines.CoroutineStart.UNDISPATCHED
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.onClosed
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.onSuccess
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.handleCoroutineException
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.whileSelect
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private val logger: Logger =
  LoggerFactory.getLogger(EventLoop::class.java)

/**
 * The EventLoop is the main loop for the KafkaReceiver.
 * It polls the KafkaConsumer, and sends the resulting records to a Channel.
 *
 * It also takes care of committing offsets to Kafka,
 * but only when the records are successfully processed by the user by calling [Offset.acknowledge] (or [Offset.commit]).
 *
 * If the user cannot process the records downstream, then the loop will call [KafkaConsumer.pause] the partitions.
 * This does not cause a group rebalance when automatic assignment is used.
 *
 * When the downstream catches up processing, the loop will call [KafkaConsumer.resume] the partitions and continue polling.
 */
internal class EventLoop<K, V>(
  private val topicNames: Collection<String>,
  private val settings: ReceiverSettings<K, V>,
  private val consumer: Consumer<K, V>,
  private val scope: CoroutineScope,
  private val outerContext: CoroutineContext,
  private val awaitingTransaction: AtomicBoolean = AtomicBoolean(false),
  private val ackMode: AckMode = MANUAL_ACK,
  private val isRetryableCommit: (Throwable) -> Boolean =
    { e -> e is RetriableCommitFailedException },
) {
  private val isPolling = AtomicBoolean(true)
  private val isPaused = AtomicBoolean(false)
  private val channel = Channel<ConsumerRecords<K, V>>()
  private val pollTimeout = settings.pollTimeout.toJavaDuration()
  private val pausedPartitionsByUser: MutableSet<TopicPartition> = HashSet()
  private val commitBatchSignal = Channel<Unit>(Channel.RENDEZVOUS)
  private val utmostOnceOffsets = UtmostOnceOffsets()

  internal fun receive(): Flow<ConsumerRecords<K, V>> =
    channel.consumeAsFlow()
      .onStart {
        if (topicNames.isNotEmpty()) subscribe(topicNames)
        schedulePoll()
        commitManager.start()
      }.onCompletion {
        commitBatchSignal.close()
        withContext(scope.coroutineContext) {
          commitManager.cancelAndJoin()
          consumer.wakeup()
          close(settings.closeTimeout)
        }
      }

  private suspend fun subscribe(topicNames: Collection<String>): Unit =
    withContext(scope.coroutineContext) {
      try {
        consumer.subscribe(topicNames, RebalanceListener())
      } catch (e: Throwable) {
        logger.error("Subscribing to $topicNames failed", e)
        closeChannel(e)
      }
    }

  //<editor-fold desc="Polling">
  /* Checks if we need to pause,
   * If it was already paused, then we check if we need to wake up the consumer.
   * We wake up the consumer, if we're actively polling records, and we're currently not retrying sending commits. */
  @ConsumerThread
  private fun pauseAndWakeupIfNeeded(): Boolean {
    checkConsumerThread("pauseAndWakeupIfNeeded")
    val pausedNow = !isPaused.getAndSet(true)
    val shouldWakeUpConsumer = pausedNow && isPolling.get() && !isRetryingCommit.get()
    logger.debug("checkAndSetPausedByUs: already paused {}, shouldWakeUpConsumer {}", pausedNow, shouldWakeUpConsumer)
    if (shouldWakeUpConsumer) {
      consumer.wakeup()
    }
    return pausedNow
  }

  private val scheduled = AtomicBoolean(false)
  private fun schedulePoll() {
    if (scheduled.compareAndSet(false, true)) {
      scope.launch {
        scheduled.set(false)
        @OptIn(DelicateCoroutinesApi::class)
        if (!channel.isClosedForSend) poll()
      }
    }
  }

  @ConsumerThread
  private fun poll() {
    checkConsumerThread("poll")
    try {
      runCommitIfRequired(false)

      val pauseForDeferredCommit =
        (settings.maxDeferredCommits > 0 && commitBatch.deferredCount() >= settings.maxDeferredCommits)

      val shouldPoll =
        if (pauseForDeferredCommit || isRetryingCommit.get()) false else isPolling.get()

      if (shouldPoll) {
        if (!awaitingTransaction.get()) {
          if (isPaused.getAndSet(false)) {
            val toResume: MutableSet<TopicPartition> = HashSet(consumer.assignment())
            toResume.removeAll(pausedPartitionsByUser)
            pausedPartitionsByUser.clear()
            consumer.resume(toResume)
            logger.debug("Resumed partitions: {}", toResume)
          }
        } else {
          if (pauseAndWakeupIfNeeded()) {
            pausedPartitionsByUser.addAll(consumer.paused())
            consumer.pause(consumer.assignment())
            logger.debug("Paused - awaiting transaction")
          }
        }
      } else if (pauseAndWakeupIfNeeded()) {
        pausedPartitionsByUser.addAll(consumer.paused())
        consumer.pause(consumer.assignment())
        when {
          pauseForDeferredCommit -> logger.debug("Paused - too many deferred commits")
          isRetryingCommit.get() -> logger.debug("Paused - commits are retrying")
          else -> logger.debug("Paused - back pressure")
        }
      }

      val records: ConsumerRecords<K, V> = try {
        consumer.poll(pollTimeout)
      } catch (e: WakeupException) {
        logger.debug("Consumer woken")
        ConsumerRecords.empty()
      }

      if (records.isEmpty) {
        schedulePoll()
      } else {
        if (settings.maxDeferredCommits > 0) {
          commitBatch.addUncommitted(records)
        }
        logger.debug("Attempting to send ${records.count()} records to Channel")
        channel.trySend(records)
          .onSuccess { schedulePoll() }
          .onClosed { error -> logger.error("Channel closed when trying to send records.", error) }
          .onFailure { error ->
            if (error != null) {
              logger.error("Channel send failed when trying to send records.", error)
              closeChannel(error)
            } else {
              logger.debug("Back-pressuring kafka consumer. Might pause KafkaConsumer on next poll tick.")

              isPolling.set(false)
              scope.launch(outerContext) {
                /* Send the records down,
                 * when send returns we attempt to send and empty set of records down to test the backpressure.
                 * If our "backpressure test" returns we start requesting/polling again. */
                channel.send(records)
                if (isPaused.get()) {
                  consumer.wakeup()
                }
                isPolling.set(true)
                schedulePoll()
              }
            }
          }
      }
    } catch (e: Exception) {
      logger.error("Polling encountered an unexpected exception", e)
      closeChannel(e)
    }
  }

  @ConsumerThread
  private inner class RebalanceListener : ConsumerRebalanceListener {
    @ConsumerThread
    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
      checkConsumerThread("RebalanceListener.onPartitionsAssigned")
      logger.debug("onPartitionsAssigned {}", partitions)
      val repausedAll = partitions.isNotEmpty() && isPaused.get()
      if (repausedAll) {
        logger.debug("Rebalance during back pressure, re-pausing new assignments")
        consumer.pause(partitions)
      }
      if (pausedPartitionsByUser.isNotEmpty()) {
        val toRepause = partitionsToRepause(partitions)
        if (!repausedAll && toRepause.isNotEmpty()) {
          consumer.pause(toRepause)
        }
      }
      // TODO Setup user listeners
      // for (onAssign in receiverOptions.assignListeners()) {
      //   onAssign.accept(toSeekable(partitions))
      // }
      traceCommitted(partitions)
    }


    @ConsumerThread
    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
      checkConsumerThread("RebalanceListener.onPartitionsRevoked")
      logger.debug("onPartitionsRevoked {}", partitions)
      partitionsRevoked(partitions)
      commitBatch.onPartitionsRevoked(partitions)
    }

    /* It is necessary to re-pause any user-paused partitions that are re-assigned after the rebalance.
     * Also remove any revoked partitions that the user paused from the userPaused collection. */
    private fun partitionsToRepause(partitions: Collection<TopicPartition>): List<TopicPartition> =
      buildList {
        pausedPartitionsByUser.forEach { tp ->
          if (partitions.contains(tp)) add(tp)
          else pausedPartitionsByUser.remove(tp)
        }
      }
  }

  @ConsumerThread
  private fun partitionsRevoked(partitions: Collection<TopicPartition>) {
    if (!partitions.isEmpty()) {
      // It is safe to use the consumer here since we are in a poll()
      if (ackMode != ATMOST_ONCE) {
        runCommitIfRequired(true)
      }
      // TODO Setup user listeners
      // for (onRevoke in receiverOptions.revokeListeners()) {
      //   onRevoke.accept(toSeekable(partitions))
      // }
    }
  }
  //</editor-fold>

  //<editor-fold desc="Comitting">
  val commitBatch: CommittableBatch = CommittableBatch()
  private val commitPending = AtomicBoolean()
  private val asyncCommitsInProgress = AtomicInteger()
  private val consecutiveCommitFailures = AtomicInteger()
  private val isRetryingCommit = AtomicBoolean()

  /**
   * If we were retrying, schedule a poll and set isRetryingCommit to false
   * If we weren't retrying, do nothing.
   */
  @ConsumerThread
  private fun schedulePollAfterRetrying() {
    if (isRetryingCommit.getAndSet(false)) poll()
  }

  @ConsumerThread
  private fun runCommitIfRequired(force: Boolean) {
    if (force) commitPending.set(true)
    if (!isRetryingCommit.get() && commitPending.get()) commit()
  }

  fun scheduleCommitIfRequired() {
    if (
      !isRetryingCommit.get() &&
      commitPending.compareAndSet(false, true)
    ) scope.launch { commit() }
  }

  // TODO  We should reset delay of commitJob
  @ConsumerThread
  private fun commit() {
    checkConsumerThread("commit")
    if (!commitPending.compareAndSet(true, false)) return
    val commitArgs: CommittableBatch.CommitArgs = commitBatch.getAndClearOffsets()
    if (commitArgs.offsets.isEmpty()) commitSuccess(commitArgs, commitArgs.offsets)
    else {
      when (ackMode) {
        MANUAL_ACK, AUTO_ACK -> commitAsync(commitArgs)
        ATMOST_ONCE -> commitSync(commitArgs)
        /** [AckMode.EXACTLY_ONCE] offsets are committed by a producer. */
        EXACTLY_ONCE -> Unit
      }
    }
  }

  /**
   * Commit async, for [MANUAL_ACK] & [AUTO_ACK].
   * We increment the [asyncCommitsInProgress] and request the SDK to [Consumer.commitAsync].
   *
   * We always decrement  [asyncCommitsInProgress],
   * and invoke the relevant handlers in case of [commitSuccess] or [commitFailure].
   *
   * We always need to poll after [Consumer.commitAsync].
   */
  private fun commitAsync(commitArgs: CommittableBatch.CommitArgs) {
    runCatching {
      asyncCommitsInProgress.incrementAndGet()
      logger.debug("Async committing: {}", commitArgs.offsets)
      consumer.commitAsync(commitArgs.offsets) { offsets, exception ->
        asyncCommitsInProgress.decrementAndGet()
        if (exception == null) commitSuccess(commitArgs, offsets)
        else commitFailure(commitArgs, exception)
      }
      poll()
    }.recoverCatching { e ->
      // TODO("NonFatal")
      asyncCommitsInProgress.decrementAndGet()
      commitFailure(commitArgs, e)
    }
  }

  /**
   * Commit sync, for [ATMOST_ONCE].
   * For [ATMOST_ONCE] we want to guarantee it's only received once,
   * so we immediately commit the message before sending it to the [Channel].
   * This is blocking, and invoke the relevant handlers in case of [commitSuccess] or [commitFailure].
   */
  private fun commitSync(commitArgs: CommittableBatch.CommitArgs) {
    runCatching {
      logger.debug("Sync committing: {}", commitArgs.offsets)
      // TODO check if this should be runInterruptible ??
      consumer.commitSync(commitArgs.offsets)
      commitSuccess(commitArgs, commitArgs.offsets)
      utmostOnceOffsets.onCommit(commitArgs.offsets)
    }.recoverCatching { e ->
      // TODO("NonFatal")
      commitFailure(commitArgs, e)
    }
  }

  /**
   * Commit was successfully:
   *   - Set commitFailures to 0
   *   - Schedule poll if we previously were retrying to commit
   *   - Complete all the [Offset.commit] continuations
   */
  @ConsumerThread
  private fun commitSuccess(
    commitArgs: CommittableBatch.CommitArgs,
    offsets: Map<TopicPartition, OffsetAndMetadata>
  ) {
    checkConsumerThread("commitSuccess")
    if (offsets.isNotEmpty()) consecutiveCommitFailures.set(0)
    schedulePollAfterRetrying()
    commitArgs.continuations?.forEach { cont ->
      cont.resume(Unit)
    }
  }

  @ConsumerThread
  private fun commitFailure(commitArgs: CommittableBatch.CommitArgs, exception: Throwable) {
    checkConsumerThread("commitFailure")
    logger.warn("Commit failed", exception)
    if (
      !isRetryableCommit(exception) &&
      consecutiveCommitFailures.incrementAndGet() < settings.maxCommitAttempts
    ) {
      logger.debug("Commit failed with exception $exception, zero retries remaining")
      schedulePollAfterRetrying()
      val continuations = commitArgs.continuations
      if (continuations.isNullOrEmpty()) {
        closeChannel(exception)
      } else {
        commitPending.set(false)
        commitBatch.restoreOffsets(commitArgs, false)
        continuations.forEach { cont ->
          cont.resumeWithException(exception)
        }
      }
    } else {
      commitBatch.restoreOffsets(commitArgs, true)
      // TODO addSuppressed if in the end we failed to commit within settings.maxCommitAttempts???
      logger.warn("Commit failed with exception $exception, retries remaining ${(settings.maxCommitAttempts - consecutiveCommitFailures.get())}")
      commitPending.set(true)
      isRetryingCommit.set(true)
      poll()
      /* We launch UNDISPATCHED as optimisation since we're already on the consumer thread,
       * and we immediately call delay. */
      scope.launch(start = UNDISPATCHED) {
        delay(settings.commitRetryInterval)
        commit()
      }
    }
  }

  /* Close the PollLoop,
   * and commit all outstanding [Offset.acknowledge] and [Offset.commit] before closing.
   * It will make `3` attempts to commit the offsets. */
  suspend fun close(timeout: Duration): Unit = withContext(scope.coroutineContext) {
    checkConsumerThread("close")
//     val manualAssignment: Collection<TopicPartition> = receiverOptions.assignment()
//     if (!manualAssignment.isEmpty()) revokePartitions(manualAssignment)

    commitOnClose(
      closeEndTime = System.currentTimeMillis() + timeout.inWholeMilliseconds,
      maxAttempts = 3
    )
  }

  /* We recurse here in case the consumer has had a recent wakeup call (from user code)
   * which will cause a poll() (in waitFor) to be interrupted while we're
   * possibly waiting for async commit results. */
  @ConsumerThread
  private /*suspend*/ fun commitOnClose(closeEndTime: Long, maxAttempts: Int) {
    try {
      val forceCommit = when (ackMode) {
        ATMOST_ONCE -> utmostOnceOffsets.undoCommitAhead(commitBatch)
        else -> true
      }
      //
      /* [AckMode.EXACTLY_ONCE] offsets are committed by a producer, consumer may be closed immediately.
       * For all other [AckMode] we need to commit all [ReceiverRecord] [Offset.acknowledge].
       * Since we want to perform this in an optimal way. */
      if (ackMode != EXACTLY_ONCE) {
        runCommitIfRequired(forceCommit)
        /* The SDK doesn't send commit requests, unless we also poll.
         * So poll for 1 millis, until no more commitsInProgress. */
        while (asyncCommitsInProgress.get() > 0 && closeEndTime - System.currentTimeMillis() > 0) {
          consumer.poll(JavaDuration.ofMillis(1))
        }
      }
      val timeoutRemaining = closeEndTime - System.currentTimeMillis()
      consumer.close(JavaDuration.ofMillis(timeoutRemaining.coerceAtLeast(0)))
    } catch (e: WakeupException) {
      if (maxAttempts == 0) throw e
      else commitOnClose(closeEndTime, maxAttempts - 1)
    }
  }
  //</editor-fold>

  //<editor-fold desc="Logging">
  /* Trace the position, and last committed offsets for the given partitions */
  @ConsumerThread
  private fun traceCommitted(partitions: Collection<TopicPartition>) {
    if (logger.isTraceEnabled) {
      try {
        val positions = partitions.map { part: TopicPartition ->
          "$part position: ${consumer.position(part, ofSeconds(5))}"
        }

        val committed =
          consumer.committed(partitions.toSet(), ofSeconds(5))

        logger.trace("positions: $positions, committed: $committed")
      } catch (ex: Exception) {
        logger.error("Failed to get positions or committed", ex)
      }
    }
  }
  //</editor-fold>

  internal fun offsetFromRecord(record: ConsumerRecord<K, V>): Offset =
    CommitOffset(
      TopicPartition(record.topic(), record.partition()),
      record.offset(),
      settings.commitStrategy.size(),
      commitBatchSignal
    )

  private inner class CommitOffset(
    override val topicPartition: TopicPartition,
    override val offset: Long,
    private val commitBatchSize: Int,
    private val reachedMaxCommitBatchSize: Channel<Unit>,
  ) : Offset {
    private val acknowledged = AtomicBoolean(false)

    // TODO should throw if called after Flow finished
    //   Or, onCommitDropped (?)
    override suspend fun commit(): Unit =
      if (maybeUpdateOffset() > 0) suspendCoroutine { cont ->
        commitBatch.addContinuation(cont)
        scheduleCommitIfRequired()
      } else Unit

    // TODO should throw if called after Flow finished
    //    Or, onCommitDropped (?)
    override suspend fun acknowledge() {
      val uncommittedCount = maybeUpdateOffset().toLong()
      if (commitBatchSize in 1..uncommittedCount) {
        reachedMaxCommitBatchSize.send(Unit)
      }
    }

    private /*suspend*/ fun maybeUpdateOffset(): Int =
      if (acknowledged.compareAndSet(false, true)) commitBatch.updateOffset(topicPartition, offset)
      else commitBatch.batchSize()

    override fun toString(): String = "$topicPartition@$offset"
  }

  @OptIn(InternalCoroutinesApi::class)
  private fun closeChannel(e: Throwable) {
    val alreadyClosed = !channel.close(e)
    if (alreadyClosed) {
      handleCoroutineException(outerContext, e)
    }
  }

  /**
   * A suspend function that will schedule [commit] based on the [AckMode], [CommitStrategy] and [commitBatchSignal].
   * Run in a parallel Job alongside the [EventLoop],
   * this way we have a separate process managing, and scheduling the commits.
   *
   * KotlinX Coroutines exposes a powerful experimental API where we can listen to our [commitBatchSignal],
   * while racing against a [onTimeout]. This allows for easily committing on whichever event arrives first.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  private val commitManager = scope.launch(start = LAZY) {
    if (ackMode == MANUAL_ACK || ackMode == AUTO_ACK) {
      whileSelect {
        when (settings.commitStrategy) {
          is CommitStrategy.BySizeOrTime -> {
            commitBatchSignal.onReceiveCatching {
              scheduleCommitIfRequired()
              !it.isClosed
            }
            onTimeout(settings.commitStrategy.interval) {
              scheduleCommitIfRequired()
              true
            }
          }

          is CommitStrategy.BySize ->
            commitBatchSignal.onReceiveCatching {
              scheduleCommitIfRequired()
              !it.isClosed
            }

          is CommitStrategy.ByTime ->
            onTimeout(settings.commitStrategy.interval) {
              scheduleCommitIfRequired()
              true
            }
        }
      }
    }
  }
}

/* Marker for functions that are only called from the KafkaConsumer thread */
private annotation class ConsumerThread

/* Testing [ConsumerThread] for internal usage.
 * Will be removed before 1.0.0 */
private const val DEBUG: Boolean = true

private fun checkConsumerThread(msg: String): Unit =
  if (DEBUG) check(
    Thread.currentThread().name.startsWith("kotlin-kafka-")
  ) { "$msg => should run on kotlin-kafka thread, but found ${Thread.currentThread().name}" }
  else Unit
