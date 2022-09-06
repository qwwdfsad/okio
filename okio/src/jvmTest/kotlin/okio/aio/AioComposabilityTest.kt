package okio.aio

import java.nio.file.*
import kotlin.io.path.*
import kotlin.test.*
import kotlinx.coroutines.*
import okio.*
import org.junit.After
import org.junit.Test

class AioComposabilityTest {

  private val expectedContent = AioComposabilityTest::class.java
    .getResourceAsStream("/okio/megabyte_of_text.txt")!!
    .buffered().readBytes().decodeToString()

  private val path = Files.createTempFile("123", "213")

  @After
  fun cleanup() {
    path.deleteExisting()
  }

  @Test
  fun sample() {
    val buffer = Buffer()
    val sink = (buffer as RawSink).gzip().buffer()
    sink.writeUtf8(expectedContent)
    sink.flush()
    sink.close()

    println("Buffer size: ${buffer.size}")

    val string = (buffer as Source).gzip().buffer().readUtf8()
    println(string.length.toString() + " " + expectedContent.length)
  }

  @Test
  fun testDoubleComposition() = runBlocking<Unit> {
    val sink = AsynchronousFileSink(path).gzip().buffer()
    sink.writeUtf8(expectedContent)
    sink.flushSuspend()
    sink.close()

    println("String size: ${expectedContent.length}, written down: ${path.fileSize()} bytes")

    val asyncSource = AsynchronousFileSource(path).asyncGzip().buffer()
    val blockingSource = Files.readAllBytes(path).inputStream().source().gzip().buffer()

    while (!blockingSource.exhausted()) {
      val expected = blockingSource.readUtf8Line()
      asyncSource.awaitAvailable(AwaitPredicate.Utf8String)
      val actual = asyncSource.readUtf8Line()
      assertEquals(expected, actual)
    }
    assertTrue { asyncSource.exhausted() }
  }

  @Test
  fun testStringComposition() = runBlocking<Unit> {
    val sink = AsynchronousFileSink(path).buffer()
    val longString = "foo".repeat(128)
    val longString2 = "bar".repeat(128)
    sink.writeUtf8(longString)
    sink.writeUtf8("\n")
    sink.writeUtf8(longString2)
    sink.writeUtf8("\n")
    sink.flushSuspend()
    sink.close()

    println("Written down: ${path.fileSize()} bytes")

    val source = AsynchronousFileSource(path).buffer()
    var totalRead = 0L
    val expected = buildList {
      val read1 = source.awaitAvailable(AwaitPredicate.Utf8String)
      totalRead += read1
      assertEquals(512, read1)
      add(source.readUtf8LineStrict())
      val read2 = source.awaitAvailable(AwaitPredicate.Utf8String)
      assertEquals(258, read2)
      add(source.readUtf8LineStrict())
      totalRead += read2
    }
    source.close()
    assertEquals(listOf(longString, longString2), expected)
    assertEquals(path.fileSize(), totalRead)
  }
}
