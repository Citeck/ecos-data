package ru.citeck.ecos.data.sql.content

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.InputStream
import java.util.Collections
import java.util.IdentityHashMap

/**
 * Holds a [ContentMimeTypeDetector] to its contract, so that no caller has to.
 *
 * A detector is supplied by the application, and recognizing a content is never the point of the
 * operation it happens inside - an upload stores the bytes either way. So an implementation that
 * throws must cost its caller the recognition and nothing else: everything it throws becomes the
 * same "nothing recognized" answer an honest implementation would have returned, and is logged
 * once, here.
 *
 * Two things are deliberately not softened:
 *
 * - an [Error] is not an answer about the content, it is the call not having happened at all, and
 *   the bytes are still there to be typed correctly on a retry;
 * - an [InterruptedException] anywhere in the cause chain means the calling *thread* is being
 *   cancelled, which says nothing about the content either. Catching it already cleared the
 *   interrupt flag, and "I don't recognize this" is indistinguishable from a successful answer -
 *   so the flag is restored and the exception rethrown, or the cancellation would be invisible to
 *   everything above and the caller would run on.
 *
 * Every detector reaching the platform is wrapped ([ru.citeck.ecos.data.sql.context
 * .DbDataSourceContext]), which is what keeps the chunked and the single-shot upload path from
 * differing over a broken detector - and what lets an implementation be written as if it could not
 * fail, because failing is handled for it.
 */
class SafeContentMimeTypeDetector(
    private val impl: ContentMimeTypeDetector
) : ContentMimeTypeDetector {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun detect(name: String, content: InputStream): MimeType? {
        return guarded({ "Content type of '$name' can't be recognized" }) { impl.detect(name, content) }
    }

    /**
     * A detector that cannot say how many bytes it needs gets no bytes: a non-positive answer is
     * what the interface already documents as "keep the type you have", so the failure needs no
     * separate shape.
     */
    override fun getPrefixSize(): Int {
        return guarded({ "Prefix size of the content type detector can't be resolved" }) { impl.getPrefixSize() } ?: 0
    }

    override fun getExtension(mimeType: MimeType): String {
        return guarded({ "Extension of '$mimeType' can't be resolved" }) { impl.getExtension(mimeType) } ?: ""
    }

    private fun <T> guarded(message: () -> String, action: () -> T): T? {
        return try {
            action()
        } catch (e: Exception) {
            rethrowIfInterrupted(e)
            log.warn(e, message)
            null
        }
    }

    /**
     * Restores the interrupt flag and rethrows when [e] is - or wraps - an [InterruptedException].
     *
     * The walk remembers where it has been, by identity: nothing stops an exception from being its
     * own cause or from being the cause of its own cause, and a chain that loops would otherwise
     * hang the call it was meant to end.
     */
    private fun rethrowIfInterrupted(e: Exception) {
        val visited = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
        var current: Throwable? = e
        while (current != null && visited.add(current)) {
            if (current is InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            }
            current = current.cause
        }
    }
}
