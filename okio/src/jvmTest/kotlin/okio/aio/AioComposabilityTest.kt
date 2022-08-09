package okio.aio

import java.nio.file.*
import kotlin.io.path.*
import kotlinx.coroutines.*
import okio.*
import okio.FileSystem
import okio.Path.Companion.toOkioPath
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
  fun doTest() = runBlocking<Unit> {
    val sink = AsynchronousFileSink(path).gzip().buffer()
    sink.writeUtf8(expectedContent)
    sink.flushSuspend()
    sink.close()

    println("String size: ${expectedContent.length}, written down: ${path.fileSize()} bytes")

    val source = AsynchronousFileSource(path).gzip().buffer()

    source.awaitAvailable()
    val result = source.readUtf8()
    source.close()

    println("String size: ${result.length}")
  }
}
