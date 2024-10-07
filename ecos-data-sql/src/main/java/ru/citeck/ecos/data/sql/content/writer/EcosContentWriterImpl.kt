package ru.citeck.ecos.data.sql.content.writer

import ru.citeck.ecos.commons.utils.ByteUtils
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.content.EcosContentWriterMeta
import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class EcosContentWriterImpl(
    private var meta: EcosContentWriterMeta,
    output: OutputStream
) : EcosContentWriter {

    private val sha256Digest = MessageDigest.getInstance("SHA-256")
    private val contentSize = AtomicLong()

    private val outputStream = OutputStreamProxy(output)

    private val finished = AtomicBoolean()

    override fun getMeta(): EcosContentWriterMeta {
        return meta
    }

    override fun setMeta(meta: EcosContentWriterMeta) {
        this.meta = meta
    }

    override fun finish(): EcosContentWriterMeta {
        if (finished.compareAndSet(false, true)) {
            if (contentSize.get() == 0L) {
                error("Empty content")
            }
            this.meta = this.meta.copy()
                .withSha256(ByteUtils.toHexString(sha256Digest.digest()))
                .withSize(contentSize.get())
                .build()
        }
        outputStream.flush()
        return meta
    }

    override fun writeText(text: String) {
        getOutputStream().write(text.toByteArray(Charsets.UTF_8))
    }

    override fun writeStream(stream: InputStream): Long {
        return stream.copyTo(getOutputStream())
    }

    override fun writeBytes(bytes: ByteArray) {
        getOutputStream().write(bytes)
    }

    override fun getOutputStream(): OutputStream {
        return outputStream
    }

    private inner class OutputStreamProxy(val impl: OutputStream) : OutputStream() {

        override fun close() {
            impl.close()
        }

        override fun flush() {
            impl.flush()
        }

        override fun write(byte: Int) {
            sha256Digest.update(byte.toByte())
            contentSize.incrementAndGet()
            impl.write(byte)
        }

        override fun write(bytes: ByteArray) {
            sha256Digest.update(bytes)
            contentSize.addAndGet(bytes.size.toLong())
            impl.write(bytes)
        }

        override fun write(bytes: ByteArray, off: Int, len: Int) {
            sha256Digest.update(bytes, off, len)
            contentSize.addAndGet(len.toLong())
            impl.write(bytes, off, len)
        }
    }
}
