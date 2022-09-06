package okio.aio

import java.io.EOFException
import java.io.IOException
import java.util.zip.*
import okio.*

class AsyncGzipSource(source: RawSource) : RawSource {

  /** The current section. Always progresses forward. */
  private var section = SECTION_HEADER

  /**
   * Our source should yield a GZIP header (which we consume directly), followed
   * by deflated bytes (which we consume via an InflaterSource), followed by a
   * GZIP trailer (which we also consume directly).
   */
  private val source = RealSource(source)

  /** The inflater used to decompress the deflated body. */
  private val inflater = Inflater(true)

  /**
   * The inflater source takes care of moving data between compressed source and
   * decompressed sink buffers.
   */
  private val inflaterSource = InflaterSource(this.source, inflater)

  /** Checksum used to check both the GZIP header and decompressed body. */
  private val crc = CRC32()


  @Throws(IOException::class)
  override fun read(sink: Buffer, byteCount: Long): Long {
    require(byteCount >= 0L) { "byteCount < 0: $byteCount" }
    if (byteCount == 0L) return 0L
    return inflatedBuffer.read(sink, byteCount)
  }

  private val inflatedBuffer = Buffer()

  @ExperimentalAsynchronousIo
  override suspend fun awaitAvailable(predicate: AwaitPredicate): Long {
    if (section == SECTION_HEADER) {
      source.awaitAvailable(AwaitPredicate.atLeastNBytes(15))
      consumeHeader()
      section = SECTION_BODY
    } else if (section == SECTION_TRAILER) {
      source.awaitAvailable(AwaitPredicate.atLeastNBytes(8))
      consumeTrailer()
      section = SECTION_DONE
      return -1L
    }

    var offset = inflatedBuffer.size
    var bytesRead = 0L
    while (!predicate.apply(inflatedBuffer, offset)) {
      // Await anything
      source.awaitAvailable()
      val result = inflaterSource.read(inflatedBuffer, Segment.SIZE.toLong())
      if (result != -1L) {
        bytesRead += result
        offset += result
        updateCrc(inflatedBuffer, offset, result)
      } else {
        return bytesRead
      }
    }
    return bytesRead.positiveOrMinisOne()
  }

  @Throws(IOException::class)
  private fun consumeHeader() {
    // Read the 10-byte header. We peek at the flags byte first so we know if we
    // need to CRC the entire header. Then we read the magic ID1ID2 sequence.
    // We can skip everything else in the first 10 bytes.
    // +---+---+---+---+---+---+---+---+---+---+
    // |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | (more-->)
    // +---+---+---+---+---+---+---+---+---+---+
    source.require(10)
    val flags = source.buffer[3].toInt()
    val fhcrc = flags.getBit(FHCRC)
    if (fhcrc) updateCrc(source.buffer, 0, 10)

    val id1id2 = source.readShort()
    checkEqual("ID1ID2", 0x1f8b, id1id2.toInt())
    source.skip(8)

    // Skip optional extra fields.
    // +---+---+=================================+
    // | XLEN  |...XLEN bytes of "extra field"...| (more-->)
    // +---+---+=================================+
    if (flags.getBit(FEXTRA)) {
      source.require(2)
      if (fhcrc) updateCrc(source.buffer, 0, 2)
      val xlen = source.buffer.readShortLe().toLong()
      source.require(xlen)
      if (fhcrc) updateCrc(source.buffer, 0, xlen)
      source.skip(xlen)
    }

    // Skip an optional 0-terminated name.
    // +=========================================+
    // |...original file name, zero-terminated...| (more-->)
    // +=========================================+
    if (flags.getBit(FNAME)) {
      val index = source.indexOf(0)
      if (index == -1L) throw EOFException()
      if (fhcrc) updateCrc(source.buffer, 0, index + 1)
      source.skip(index + 1)
    }

    // Skip an optional 0-terminated comment.
    // +===================================+
    // |...file comment, zero-terminated...| (more-->)
    // +===================================+
    if (flags.getBit(FCOMMENT)) {
      val index = source.indexOf(0)
      if (index == -1L) throw EOFException()
      if (fhcrc) updateCrc(source.buffer, 0, index + 1)
      source.skip(index + 1)
    }

    // Confirm the optional header CRC.
    // +---+---+
    // | CRC16 |
    // +---+---+
    if (fhcrc) {
      checkEqual("FHCRC", source.readShortLe().toInt(), crc.value.toShort().toInt())
      crc.reset()
    }
  }

  @Throws(IOException::class)
  private fun consumeTrailer() {
    // Read the eight-byte trailer. Confirm the body's CRC and size.
    // +---+---+---+---+---+---+---+---+
    // |     CRC32     |     ISIZE     |
    // +---+---+---+---+---+---+---+---+
    checkEqual("CRC", source.readIntLe(), crc.value.toInt())
    checkEqual("ISIZE", source.readIntLe(), inflater.bytesWritten.toInt())
  }

  override fun cancel() {
    source.cancel()
  }

  @Throws(IOException::class)
  override fun close() = inflaterSource.close()

  /** Updates the CRC with the given bytes.  */
  private fun updateCrc(buffer: Buffer, offset: Long, byteCount: Long) {
    var offset = offset
    var byteCount = byteCount
    // Skip segments that we aren't checksumming.
    var s = buffer.head!!
    while (offset >= s.limit - s.pos) {
      offset -= s.limit - s.pos
      s = s.next!!
    }

    // Checksum one segment at a time.
    while (byteCount > 0) {
      val pos = (s.pos + offset).toInt()
      val toUpdate = minOf(s.limit - pos, byteCount).toInt()
      crc.update(s.data, pos, toUpdate)
      byteCount -= toUpdate
      offset = 0
      s = s.next!!
    }
  }

  private fun checkEqual(name: String, expected: Int, actual: Int) {
    if (actual != expected) {
      throw IOException("%s: actual 0x%08x != expected 0x%08x".format(name, actual, expected))
    }
  }
}

inline fun RawSource.asyncGzip() = AsyncGzipSource(this)
