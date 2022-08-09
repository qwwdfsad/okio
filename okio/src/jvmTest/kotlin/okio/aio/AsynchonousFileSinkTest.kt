package okio.aio

import java.nio.file.*
import kotlin.io.path.*
import kotlin.test.*
import kotlinx.coroutines.*
import okio.*
import org.junit.Test

class AsynchonousFileSinkTest {

  @Test
  fun testRead() = runBlocking<Unit> {
    val path =  Files.createTempFile("testRead", "")
    val sink = AsynchronousFileSink(path).buffer()
    val resultingString = buildString {
      repeat(100) {
        sink.writeUtf8(it.toString())
        sink.writeUtf8("\n")
        appendLine(it.toString())
        sink.flushSuspend()
      }

      val longString = "a".repeat(500)
      append(longString)
      sink.writeUtf8(longString)
      sink.flushSuspend()
      sink.close()
    }

    val readString = Files.readAllBytes(path).decodeToString()
    assertEquals(resultingString, readString)
  }
}
