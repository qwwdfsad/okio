package okio.aio

import java.nio.*
import okio.*
import java.nio.channels.*
import java.nio.file.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.*
import kotlin.math.*
import okio.Buffer

/**
 * Demonstration of the implementation of the source backed by asynchronous file sink
 * without affecting composability of operation.
 *
 * The underlying implementation leverages no internal API and only uses unsafe cursor for zero copy
 * asyncrhonous reading.
 */
class AsynchronousFileSink(
  public val path: java.nio.file.Path
) : RawSink {

  private val channel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE)
  private var position = 0L
  private val temporaryBuffer = Buffer()

  override fun write(source: Buffer, byteCount: Long) {
    require(channel.isOpen) { "Sink is closed" }
    temporaryBuffer.write(source, byteCount)
    // In real world we could've auto-flushed asynchornously and rendezvous with flushSuspend if necessary
  }

  override suspend fun flushSuspend() {
    while (!temporaryBuffer.exhausted()) {
      suspendCoroutine<Unit> {
        val continuation = it
        val cursor = temporaryBuffer.readUnsafe()
        cursor.next()
        channel.write(
          ByteBuffer.wrap(cursor.data!!, cursor.start, cursor.end - cursor.start),
          position,
          this,
          object : CompletionHandler<Int, AsynchronousFileSink> {
            override fun completed(result: Int, attachment: AsynchronousFileSink) {
              position += result
              cursor.close()
              temporaryBuffer.skip(result.toLong())
              continuation.resume(Unit)
            }

            override fun failed(exc: Throwable?, attachment: AsynchronousFileSink?) {
              // TODO
            }
          })
      }
    }
  }

  override fun flush() {
    // TODO
  }

  override fun cancel() {
    channel.close()
  }

  override fun close() {
    // TODO proper implementation should flush properly first, but I'm too lazy to impl it as well
    channel.close()
  }
}
