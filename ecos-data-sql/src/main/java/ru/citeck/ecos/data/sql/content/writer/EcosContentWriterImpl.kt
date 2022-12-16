package ru.citeck.ecos.data.sql.content.writer

import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class EcosContentWriterImpl(output: OutputStream) : EcosContentWriter {

    private val sha256Digest = MessageDigest.getInstance("SHA-256")
    private val contentSize = AtomicLong()

    private val cachedHash = AtomicReference<String>()
    private val cachedHashContentSize = AtomicLong(0)

    private val outputStream = OutputStreamProxy(output)

    override fun getContentSize(): Long {
        return contentSize.get()
    }

    override fun getSha256(): String {
        if (contentSize.get() == 0L) {
            error("Empty content")
        }
        if (cachedHashContentSize.get() != contentSize.get()) {
            cachedHash.set(bytesToHex(sha256Digest.digest()))
            cachedHashContentSize.set(contentSize.get())
        }
        return cachedHash.get()
    }

    private fun bytesToHex(hash: ByteArray): String {
        val hexString = StringBuilder()
        for (b in hash) {
            val hex = Integer.toHexString(0xFF and b.toInt())
            if (hex.length == 1) {
                hexString.append('0')
            }
            hexString.append(hex)
        }
        return hexString.toString()
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
