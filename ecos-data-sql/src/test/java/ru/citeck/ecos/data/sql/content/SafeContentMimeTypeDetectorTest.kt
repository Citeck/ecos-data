package ru.citeck.ecos.data.sql.content

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

/**
 * The wrapper is what lets every caller of a [ContentMimeTypeDetector] call it as if it could not
 * fail, so what it absorbs and what it lets through is the whole of its contract.
 */
class SafeContentMimeTypeDetectorTest {

    @AfterEach
    fun clearInterrupt() {
        // a test that leaves the flag set would cancel whatever runs next in this thread
        Thread.interrupted()
    }

    private class FailingDetector(private val failure: () -> Throwable) : ContentMimeTypeDetector {
        override fun detect(name: String, content: InputStream): MimeType? = throw failure()
        override fun getPrefixSize(): Int = throw failure()
        override fun getExtension(mimeType: MimeType): String = throw failure()
    }

    private fun detector(failure: () -> Throwable) = SafeContentMimeTypeDetector(FailingDetector(failure))

    private fun callAll(detector: ContentMimeTypeDetector): List<() -> Any?> = listOf(
        { detector.detect("file.bin", ByteArrayInputStream(ByteArray(4))) },
        { detector.getPrefixSize() },
        { detector.getExtension(MimeTypes.IMG_PNG) }
    )

    @Test
    fun `an answering detector is passed through untouched`() {
        val impl = object : ContentMimeTypeDetector {
            override fun detect(name: String, content: InputStream): MimeType = MimeTypes.IMG_PNG
            override fun getPrefixSize(): Int = 4096
            override fun getExtension(mimeType: MimeType): String = ".png"
        }
        val safe = SafeContentMimeTypeDetector(impl)

        assertThat(safe.detect("file", ByteArrayInputStream(ByteArray(0)))).isEqualTo(MimeTypes.IMG_PNG)
        assertThat(safe.getPrefixSize()).isEqualTo(4096)
        assertThat(safe.getExtension(MimeTypes.IMG_PNG)).isEqualTo(".png")
    }

    /**
     * Every member answers what an honest detector recognizing nothing would have answered - for
     * the prefix size that is a non-positive value, which the interface already documents as "keep
     * the type you have".
     */
    @Test
    fun `an exception costs the recognition and nothing else`() {
        val safe = detector { IOException("storage is unavailable") }

        assertThat(safe.detect("file.bin", ByteArrayInputStream(ByteArray(4)))).isNull()
        assertThat(safe.getPrefixSize()).isNotPositive
        assertThat(safe.getExtension(MimeTypes.IMG_PNG)).isEmpty()
    }

    /**
     * An Error is the call not having happened at all, not an answer about the content: absorbing it
     * would store a type nobody computed, and the bytes are still there to be typed on a retry.
     */
    @Test
    fun `an Error is not absorbed`() {
        val safe = detector { StackOverflowError("broken beyond giving an answer") }

        callAll(safe).forEach { call ->
            assertThatThrownBy { call() }.isInstanceOf(StackOverflowError::class.java)
        }
    }

    @Test
    fun `an interrupt is rethrown with the flag restored`() {
        val failure = InterruptedException("the caller was cancelled")
        val safe = detector { failure }

        callAll(safe).forEach { call ->
            assertThatThrownBy { call() }.isSameAs(failure)
            assertThat(Thread.interrupted()).describedAs("interrupt flag of the caller").isTrue()
        }
    }

    @Test
    fun `an interrupt wrapped in another exception is rethrown too`() {
        val failure = IllegalStateException("wrapped", RuntimeException(InterruptedException("cancelled")))
        val safe = detector { failure }

        assertThatThrownBy { safe.detect("file.bin", ByteArrayInputStream(ByteArray(4))) }.isSameAs(failure)
        assertThat(Thread.interrupted()).describedAs("interrupt flag of the caller").isTrue()
    }

    /**
     * Nothing stops an exception from being its own cause, and a chain that loops would hang the
     * call the walk was meant to end - on the completion path, with the connection held open.
     */
    @Test
    fun `a cause chain that loops is walked once, not forever`() {
        val looping = object : RuntimeException("loops back on itself") {
            override val cause: Throwable get() = this
        }
        val safe = detector { looping }
        val answer = AtomicReference<Any?>()

        val thread = Thread { answer.set(safe.detect("file.bin", ByteArrayInputStream(ByteArray(4)))) }
        thread.start()
        thread.join(TimeUnit.SECONDS.toMillis(10))

        if (thread.isAlive) {
            thread.interrupt()
            throw TimeoutException("the walk over a looping cause chain did not end")
        }
        assertThat(answer.get()).describedAs("answer for a detector failing with a looping cause chain").isNull()
    }
}
