package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.InputStream

/**
 * Recognizes the mime type of a content from the content itself, for the cases where the type the
 * client stated is missing or unusable and the bytes are the only remaining source of truth.
 *
 * Sniffing needs a mime-type database, which this library deliberately doesn't depend on, so the
 * capability is inverted instead of imported: an application that has one supplies an
 * implementation (wired in through [ru.citeck.ecos.data.sql.domain.DbDomainFactory]), and without
 * one every caller here keeps behaving as if the client-stated type were final.
 *
 * Implementations are shared between threads and must be safe to call concurrently.
 *
 * No member is expected to throw for anything it was asked about - an input it cannot make sense of
 * is reported by the return value, and the input is whatever somebody uploaded, so arbitrary garbage
 * is ordinary input rather than an error. An implementation that throws anyway costs its caller the
 * recognition and nothing else: every detector the platform uses is wrapped in
 * [SafeContentMimeTypeDetector], which turns a failure into the same answer an honest
 * implementation would have given. So this contract does not have to be defended in the
 * implementation either - the one thing an implementation must not do is swallow an
 * [InterruptedException], which is about the calling thread rather than about the content and is
 * the wrapper's to propagate.
 */
interface ContentMimeTypeDetector {

    /**
     * Recognizes the mime type of a content by its first bytes and its file name.
     *
     * [content] is only a prefix of the content - at most [getPrefixSize] bytes - and the caller keeps
     * ownership of it: an implementation must neither expect the remaining bytes to become
     * available nor close the stream. A shorter stream (a content smaller than the prefix, or an
     * empty one) is normal input, not an error.
     *
     * The input is whatever was uploaded, so an unrecognizable content is an ordinary answer, not
     * an error: it is reported by the return value.
     *
     * @param name file name of the content, possibly blank and possibly without any extension
     * @param content prefix of the content, positioned at its first byte
     * @return recognized mime type, or `null` when the type could not be recognized.
     *         `application/octet-stream` means the same thing as `null` for a caller and is an
     *         equally valid answer - both leave the caller with the type it already had.
     */
    fun detect(name: String, content: InputStream): MimeType?

    /**
     * Amount of leading bytes of a content that is enough to recognize its type, and therefore
     * the size of the prefix callers pass to [detect].
     *
     * Must be positive. A caller that gets a non-positive answer has no prefix to detect on and
     * keeps the type it already had, exactly as it does when no detector is configured at all.
     */
    fun getPrefixSize(): Int

    /**
     * The default file extension of [mimeType], as it should be appended to a file name that has
     * none - so the result carries its own separator (`.png`, not `png`).
     *
     * @return the extension, or an empty string when this detector knows no extension for
     *         [mimeType]. An empty string leaves the caller's file name untouched.
     */
    fun getExtension(mimeType: MimeType): String
}
