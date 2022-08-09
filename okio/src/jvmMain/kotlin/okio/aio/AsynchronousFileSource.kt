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

    /*
     * Here we do real blocking call in order to compose this source with GzipSource.
     * We have a composability problem for the following API:
     * ```
     * val source = asyncSource.gzip().anyOtherAdapter()
     *
     * source.awaitAvailable(1) // Or 2, 3, 10k or w/e
     * source.readUtf8String()
     * ```
     *
     * `gzip` may require _an arbitrary_ number of bytes being read in order to give 1 byte in return and
     * we have no control over such implementations. So 'async' primitives wuth non-trivial dependency here
     * either should throw (e.g. Ktor may want to do that, or to provide adapters) or do block, which is also acceptable.
     *
     * It is probably okay for low-level IO where a proper implementation can await enough bytes before proceeding further,
     * but it still can be considered a mine field for end users and requires an effort around that.
     */
    if (temporaryBuffer.exhausted()) {
      // Here it is possible to read directly to 'sink' tho
      readBlocking()
    }

//    check(!temporaryBuffer.exhausted()) { "All read() calls should be preceded by 'awaitAvailable()'" }
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
  private var cursor: Buffer.UnsafeCursor? = null

  /**
   * Awaits until bytes are available, triggering an
   * internal reading process with chunk size of 128 if
   * there are no bytes available to consume.
   */
  override suspend fun awaitAvailable(atLeastBytes: Long): Long {
    // Not the conceptual implementation, but requries quite a few careful segments manipulation.
    // This is purely technical constraint, and I have discovered a truly marvelous proof of this, which this comment is too narrow to contain.
    require(atLeastBytes == -1L) {
      "Implementation supports only '-1' aka 'give me at least 1 byte if possible, but details of the buffer are unknown'"
    }
    if (!channel.isOpen || currentPosition == channel.size()) return -1
    if (temporaryBuffer.size != 0L) return temporaryBuffer.size
    /*
     * This is not really thread-safe in terms of concurrent await call, it isn't the point of this showcase,
     * though can be done via careful CAS'ing and awaiting (e.g. on kotlinx-coroutines lazily allocated deferred),
     * though the primary use-case expects this method not to be thread-safe.
     */
    return suspendCoroutine<Long> {
      continuation = it
      val cursor = temporaryBuffer.readAndWriteUnsafe()
      this.cursor = cursor
      // TODO: properly work with multiple segment and sizes, not the point of demonstration
      cursor.resizeBuffer(128)
      channel.read(
        ByteBuffer.wrap(cursor.data!!, cursor.start, cursor.end - cursor.start),
        currentPosition,
        this,
        object : CompletionHandler<Int, AsynchronousFileSource> {
          override fun completed(result: Int, attachment: AsynchronousFileSource) {
            val previousPosition = attachment.currentPosition
            attachment.currentPosition += result
            attachment.cursor!!.resizeBuffer(result.toLong())
            attachment.cursor!!.close()

            attachment.continuation!!.resumeWith(Result.success(result.toLong() - previousPosition))
          }

          override fun failed(exc: Throwable, attachment: AsynchronousFileSource?) {
            if (exc is AsynchronousCloseException) return // Expected result
            // Also not relevant for the showcase. Store it in the field and throw from read, piece of cake
            TODO()
          }
        })
    }
  }
}
