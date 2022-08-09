/*
 * Copyright (C) 2019 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okio

/**
 * Receives a stream of bytes. Use this interface to write data wherever it's needed: to the
 * network, storage, or a buffer in memory. Sinks may be layered to transform received data, such as
 * to compress, encrypt, throttle, or add protocol framing.
 *
 * Most application code shouldn't operate on a sink directly, but rather on a [Sink] which
 * is both more efficient and more convenient. Use [buffer] to wrap any sink with a buffer.
 *
 * Sinks are easy to test: just use a [Buffer] in your tests, and read from it to confirm it
 * received the data that was expected.
 *
 * ### Comparison with OutputStream
 *
 * This interface is functionally equivalent to [java.io.OutputStream].
 *
 * `OutputStream` requires multiple layers when emitted data is heterogeneous: a `DataOutputStream`
 * for primitive values, a `BufferedOutputStream` for buffering, and `OutputStreamWriter` for
 * charset encoding. This library uses `BufferedSink` for all of the above.
 *
 * RawSink is also easier to layer: there is no [write()][java.io.OutputStream.write] method that is
 * awkward to implement efficiently.
 *
 * ### Interop with OutputStream
 *
 * Use [sink] to adapt an `OutputStream` to a sink. Use [outputStream()][Sink.outputStream]
 * to adapt a sink to an `OutputStream`.
 */
expect interface RawSink : Closeable {
  /** Removes `byteCount` bytes from `source` and appends them to this.  */
  @Throws(IOException::class)
  fun write(source: Buffer, byteCount: Long)

  /** Pushes all buffered bytes to their final destination.  */
  @Throws(IOException::class)
  fun flush()


  /**
   * Pushes all buffered bytes to their destination in an asynchronous manner,
   * suspending the caller unless all data is written down.
   *
   * ### List of open questions
   *
   * * Should it be aliased to [flush] by default?
   *   Basicslly same issue as for source: throwing by default may break composability,
   *       flush by default will break composability as well, forcing users to implemnt it may break convinience
   * * Should we have an attribute for sink that distringuishes whether it supports suspendability?
   *   My vote is for 'no' -- suspendability is a secondary API extension that is supposed to be used by advance users,
   *   they will know themselves. Everyone else is not expected to use it anyway, ideally we would like to even hide it
   *   under different name, but alas it breaks composability as well.
   */
  suspend fun flushSuspend() /* = flush() */

  /**
   * Asynchronously cancel this source. Any [write] or [flush] in flight should immediately fail
   * with an [IOException], and any future writes and flushes should also immediately fail with an
   * [IOException].
   *
   * Note that it is always necessary to call [close] on a sink, even if it has been canceled.
   */
  fun cancel()

  /**
   * Pushes all buffered bytes to their final destination and releases the resources held by this
   * sink. It is an error to write a closed sink. It is safe to close a sink more than once.
   */
  @Throws(IOException::class)
  override fun close()

}
