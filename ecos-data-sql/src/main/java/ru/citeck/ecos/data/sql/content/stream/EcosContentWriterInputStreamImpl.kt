package ru.citeck.ecos.data.sql.content.stream

import java.io.InputStream
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class EcosContentWriterInputStreamImpl(private val delegate: InputStream) : EcosContentWriterInputStream() {

    private val sha256Digest = MessageDigest.getInstance("SHA-256")
    private val contentSize = AtomicLong()

    private val cachedHash = AtomicReference<String>()
    private val cachedHashContentSize = AtomicLong(0)

    override fun getSha256Digest(): String {
        if (cachedHashContentSize.get() != contentSize.get()) {
            cachedHash.set(bytesToHex(sha256Digest.digest()))
            cachedHashContentSize.set(contentSize.get())
        }
        return cachedHash.get()
    }

    override fun getContentSize(): Long {
        return contentSize.get()
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

    override fun read(): Int {
        val byte = delegate.read()
        sha256Digest.update(byte.toByte())
        contentSize.incrementAndGet()
        return byte
    }

    override fun read(bytes: ByteArray): Int {
        val readCount = delegate.read(bytes)
        if (readCount <= 0) {
            return readCount
        }
        sha256Digest.update(bytes, 0, readCount)
        contentSize.addAndGet(readCount.toLong())
        return readCount
    }

    override fun read(bytes: ByteArray, off: Int, len: Int): Int {
        val readCount = delegate.read(bytes, off, len)
        if (readCount <= 0) {
            return readCount
        }
        sha256Digest.update(bytes, off, readCount)
        contentSize.addAndGet(readCount.toLong())
        return readCount
    }

    override fun skip(n: Long): Long {
        return delegate.skip(n)
    }

    override fun available(): Int {
        return delegate.available()
    }

    override fun mark(readlimit: Int) {
        delegate.mark(readlimit)
    }

    override fun reset() {
        sha256Digest.reset()
        contentSize.set(0)
        cachedHash.set(null)
        cachedHashContentSize.set(0)
        delegate.reset()
    }

    override fun markSupported(): Boolean {
        return delegate.markSupported()
    }

    override fun close() {
        delegate.close()
    }
}
