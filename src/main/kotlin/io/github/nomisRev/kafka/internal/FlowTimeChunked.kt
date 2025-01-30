/*
 * Inspired by https://github.com/Kotlin/kotlinx.coroutines/pull/2378
 */
package io.github.nomisRev.kafka.internal

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.getOrElse
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.whileSelect
import kotlinx.coroutines.selects.onTimeout
import kotlin.time.Duration

/**
 * Chunk [Flow] until [size] elements are collected, or until a certain [duration] has passed.
 * When the [Flow] completes or throws an exception
 *
 * Groups emissions from this [Flow] into [List] . Time based implementations
 * collect upstream and emit to downstream in separate coroutines - concurrently, like Flow.buffer() operator.
 * Exact timing of emissions is not guaranteed, as it depends on collector coroutine availability.
 *
 * Size based chunking happens in a single coroutine and is purely sequential.
 *
 * Emissions always preserve order.
 * Collects upstream into a buffer and emits its content as a list at every interval or when its buffer reaches
 * maximum size. When upstream completes (or is empty), it will try to emit immediately what is left of
 * a chunk, omitting the interval and maxSize constraints.
 *
 * @param duration Interval between emissions in milliseconds. Every emission happens only after
 * interval passes, unless upstream Flow completes sooner or maximum size of a chunk is reached.
 *
 * @param size Maximum size of a single chunk. If reached, it will try to emit a chunk, ignoring the
 * interval constraint. If so happens, time-to-next-chunk gets reset to the interval value.
 */
@ExperimentalCoroutinesApi
@Deprecated("Will no longer be part of kotlin-kafka.")
public fun <T> Flow<T>.chunked(
  size: Int,
  duration: Duration,
): Flow<List<T>> {
  require(size > 0) { "Cannot create chunks smaller than 0 but found $size" }
  require(!duration.isNegative() && duration != Duration.ZERO) { "Chunk duration should be positive non-zero duration" }
  return flow {
    coroutineScope {
      val emitNowAndMaybeContinue = Channel<Boolean>(capacity = Channel.RENDEZVOUS)
      val elements = produce(capacity = size) {
        collect { element ->
          val hasCapacity = channel.trySend(element).isSuccess
          if (!hasCapacity) {
            emitNowAndMaybeContinue.send(true)
            channel.send(element)
          }
        }
        emitNowAndMaybeContinue.send(false)
      }

      whileSelect {
        emitNowAndMaybeContinue.onReceive { shouldContinue ->
          val chunk = elements.drain(maxElements = size)
          if (chunk.isNotEmpty()) emit(chunk)
          shouldContinue
        }

        onTimeout(duration) {
          val chunk: List<T> = elements.drain(maxElements = size)
          if (chunk.isNotEmpty()) emit(chunk)
          true
        }
      }
    }
  }
}

private tailrec fun <T> ReceiveChannel<T>.drain(
  acc: MutableList<T> = mutableListOf(),
  maxElements: Int,
): List<T> =
  if (acc.size == maxElements) acc
  else {
    val nextValue = tryReceive().getOrElse { error: Throwable? -> error?.let { throw (it) } ?: return acc }
    acc.add(nextValue)
    drain(acc, maxElements)
  }
