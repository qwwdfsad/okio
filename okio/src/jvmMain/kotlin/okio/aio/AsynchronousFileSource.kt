package okio.aio

import java.nio.*
import okio.*
import java.nio.channels.*
import kotlin.coroutines.*
import kotlin.math.*
import okio.Buffer

/**
 * Demonstration of the implementation of the source backed by asynchronous file source
 * without affecting composability of operations (i.e. `AFS(path).buffer()` works like a charm)
 * and affecting performance of other performance-sensitive operations (e.g. reads of individual bytes).
 *
 * The underlying implementation leverages no internal API and only uses unsafe cursor for zero copy
 * asyncrhonous reading.
 */
class AsynchronousFileSource(
  public val path: java.nio.file.Path
) : RawSource {

  private val channel = AsynchronousFileChannel.open(path)
  private val temporaryBuffer = Buffer()

  // Position in channel to read from
  private var currentPosition = 0L

  override fun read(sink: Buffer, byteCount: Long): Long {
    if (!channel.isOpen || (currentPosition == channel.size() && temporaryBuffer.exhausted())) {
      channel.close()
      return -1
    }

    check(!temporaryBuffer.exhausted()) { "All read() calls should be preceded by 'awaitAvailable()'" }
    val totalRead = temporaryBuffer.read(sink, byteCount)
    check(totalRead != -1L) { "Assertion failure, read: $totalRead" }
    return totalRead
  }

  private fun readBlocking() {
    temporaryBuffer.readAndWriteUnsafe().use {
      it.resizeBuffer(128)
      val future = channel.read(ByteBuffer.wrap(it.data!!, it.start, it.end - it.start), currentPosition)
      val read = future.get()
      it.resizeBuffer(read.toLong())
      currentPosition += read
    }
  }

  override fun cancel() {
    channel.close()
  }

  override fun close() {
    channel.close()
  }

  // To save an allocation of CompletionHandler
  private var continuation: Continuation<Long>? = null

  @ExperimentalAsynchronousIo
  override suspend fun awaitAvailable(predicate: AwaitPredicate): Long {
    if (!channel.isOpen || currentPosition == channel.size()) return -1
    var lastAwaitedIndex = 0L
    var totalRead = 0L
    while (temporaryBuffer.size == 0L || !predicate.apply(temporaryBuffer, lastAwaitedIndex)) {
      lastAwaitedIndex = temporaryBuffer.size
      val byteArray = ByteArray(128) // TODO make it zero-copy when tests pass
      val bytesRead = suspendCoroutine<Long> {
        continuation = it
        val buffer = ByteBuffer.wrap(byteArray)
        channel.read(buffer, currentPosition, this, AFSHandler)
      }
      if (bytesRead == -1L) return totalRead.positiveOrMinisOne()
      totalRead += bytesRead
      // TODO make zero-copy via writeableSegment(1)
      temporaryBuffer.write(byteArray, 0, bytesRead.toInt())
    }
    return totalRead.positiveOrMinisOne()
  }


  private object AFSHandler : CompletionHandler<Int, AsynchronousFileSource> {
    override fun completed(result: Int, attachment: AsynchronousFileSource) {
      attachment.currentPosition += result
      attachment.continuation!!.resumeWith(Result.success(result.toLong()))
    }

    override fun failed(exc: Throwable, attachment: AsynchronousFileSource) {
      val cont = attachment.continuation!!
      if (exc is AsynchronousCloseException) {
        cont.resumeWith(Result.success(-1L))
      } else {
        cont.resumeWithException(exc)
      }
    }
  }
}

internal fun Long.positiveOrMinisOne() = if (this > 0) this else -1L

