package okio.aio

import java.nio.file.*
import kotlin.io.path.*
import kotlin.test.*
import kotlinx.coroutines.*
import okio.*
import org.junit.Test

class AsynchonousFileSourceTest {

  @Test
  fun testRead() = runBlocking<Unit> {
    val string = buildString {
      repeat(100) {
        appendLine(it.toString())
      }
    }
    val path = prepareFile(string)

    val source = AsynchronousFileSource(path).buffer()
    source.awaitAvailable()
    val result = buildString {
      while (!source.exhausted()) {
        append(source.readUtf8Line())
        append("\n")
        source.awaitAvailable()
      }
    }
    assertEquals(string, result)
  }

  private fun prepareFile(string: String): java.nio.file.Path {
    val f =  Files.createTempFile("testRead", "")
    f.writeBytes(string.encodeToByteArray())
    return f
  }
}
