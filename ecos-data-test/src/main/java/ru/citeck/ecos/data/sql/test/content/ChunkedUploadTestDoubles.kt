package ru.citeck.ecos.data.sql.test.content

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.ContentMimeTypeDetector
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitMeta
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitResult
import ru.citeck.ecos.data.sql.content.storage.ChunkedUploadGoneException
import ru.citeck.ecos.data.sql.content.storage.EcosContentChunkedStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorage
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * In-memory double for [EcosContentStorage] + [EcosContentChunkedStorage]: accumulates chunk
 * bytes per upload `state`, keyed by `chunkIndex` (overwrite-by-index, matching the real
 * contract - a re-sent chunk at the same index must replace, not append), and materializes them
 * in index order under a fresh `dataKey` on [chunkedComplete], readable back via [readContent].
 * [writeBarrier], when set, makes [chunkedWriteChunk] rendezvous with another thread before
 * returning - used to widen the race window for the concurrent chunk-write test.
 * [completeDelayMs], when set, makes [chunkedComplete] pause before returning - used to widen the
 * race window for the concurrent-complete test. Every chunked call asserts it was addressed to
 * [expectedStorageRef]: the storage ref is the only thing that identifies a chunked upload's
 * target (see [EcosContentChunkedStorage]), and it is otherwise easy to silently carry the wrong
 * one over from `init` into the later calls.
 */
internal class FakeChunkedStorage(
    /**
     * Effective chunk size returned from [chunkedInit]; mutable so a test can make it invalid.
     */
    @Volatile var chunkSize: Long,
    private val expectedStorageRef: EntityRef
) : EcosContentStorage,
    EcosContentChunkedStorage {

    private val active = ConcurrentHashMap<String, ConcurrentHashMap<Int, ByteArray>>()
    private val completed = ConcurrentHashMap<String, ByteArray>()
    private val completedKeyByState = ConcurrentHashMap<String, String>()
    private val stateSeq = AtomicLong()

    @Volatile
    var writeBarrier: CyclicBarrier? = null

    @Volatile
    var completeDelayMs: Long = 0

    /**
     * When set, the next [chunkedWriteChunk]/[chunkedComplete]/[chunkedAbort] call throws
     * [ChunkedUploadGoneException] instead of doing its usual thing - simulating what
     * [ru.citeck.ecos.data.sql.content.storage.remote.EcosContentRemoteStorage] would already
     * have translated a swept-away remote upload into (translation itself is covered by
     * `EcosContentRemoteStorageChunkedTest`, not here).
     */
    @Volatile
    var goneOnNextCall = false

    /**
     * When set, [chunkedComplete] throws [ChunkedUploadGoneException] *after* [completeDelayMs] has
     * elapsed rather than on entry, so a test can act on the session while the completer is still
     * inside the storage and only then let that completer fail - which is the shape of a storage
     * that consumed the upload for someone else while this one was assembling it.
     */
    @Volatile
    var goneOnCompleteAfterDelay = false

    /**
     * When set, [readContent] throws it instead of serving the stored bytes - simulating a
     * storage that took the upload and then stopped answering reads.
     */
    @Volatile
    var readFailure: RuntimeException? = null

    val completedCount: Int get() = completed.size

    private val completeCalls = AtomicInteger()

    /**
     * How many times [chunkedComplete] was entered - as opposed to [completedCount], which counts
     * assembled objects and stays at 1 however often the idempotent complete is repeated for the
     * same state. Only the call count can tell whether a caller was stopped *before* it reached the
     * storage.
     */
    val chunkedCompleteCalls: Int get() = completeCalls.get()
    val activeCount: Int get() = active.size

    private val singleShotUploads = AtomicLong()

    private val reads = AtomicLong()

    private val chunkWrites = AtomicLong()

    /**
     * How many chunks were actually handed to this storage.
     */
    val chunkWriteCount: Long get() = chunkWrites.get()

    /**
     * How many times the stored bytes were handed back to a reader, whatever the range.
     */
    val readCount: Long get() = reads.get()

    /**
     * How many times [uploadContent] was called, i.e. how many single-shot uploads this storage
     * served.
     */
    val singleShotUploadCount: Long get() = singleShotUploads.get()

    private fun maybeThrowGone(op: String) {
        if (goneOnNextCall) {
            goneOnNextCall = false
            throw ChunkedUploadGoneException("Fake storage: upload is gone ($op)")
        }
    }

    private fun checkStorageRef(storageRef: EntityRef, op: String) {
        check(storageRef == expectedStorageRef) {
            "wrong storage on $op: expected $expectedStorageRef but got $storageRef"
        }
    }

    /**
     * Not part of the chunked orchestration, but the same storage also has to serve the
     * single-shot upload the chunked path is compared against.
     */
    override fun uploadContent(
        storageRef: EntityRef,
        storageConfig: ObjectData,
        content: (OutputStream) -> Unit
    ): String {
        singleShotUploads.incrementAndGet()
        val out = ByteArrayOutputStream()
        content(out)
        val dataKey = "single-shot-" + stateSeq.incrementAndGet()
        completed[dataKey] = out.toByteArray()
        return dataKey
    }

    override fun <T> readContent(
        storageRef: EntityRef,
        dataKey: String,
        range: ContentRange,
        action: (InputStream) -> T
    ): T {
        readFailure?.let { throw it }
        reads.incrementAndGet()
        val bytes = completed[dataKey] ?: error("No fake data for key '$dataKey'")
        return action(range.apply(ByteArrayInputStream(bytes)))
    }

    override fun deleteContent(storageRef: EntityRef, dataKey: String) {
        completed.remove(dataKey)
    }

    override fun chunkedInit(
        storageRef: EntityRef,
        meta: ChunkedInitMeta
    ): ChunkedInitResult {
        checkStorageRef(storageRef, "init")
        val state = "upload-" + stateSeq.incrementAndGet()
        active[state] = ConcurrentHashMap()
        return ChunkedInitResult(true, state, chunkSize)
    }

    override fun chunkedWriteChunk(
        storageRef: EntityRef,
        state: String,
        chunkIndex: Int,
        content: InputStream,
        contentLength: Long
    ): String {
        checkStorageRef(storageRef, "writeChunk")
        chunkWrites.incrementAndGet()
        maybeThrowGone("writeChunk")
        writeBarrier?.await(10, TimeUnit.SECONDS)
        val chunks = active[state] ?: error("Unknown chunked-upload state: '$state'")
        chunks[chunkIndex] = content.readBytes()
        return state
    }

    override fun chunkedComplete(storageRef: EntityRef, state: String): String {
        completeCalls.incrementAndGet()
        checkStorageRef(storageRef, "complete")
        maybeThrowGone("complete")
        val delay = completeDelayMs
        if (delay > 0) {
            Thread.sleep(delay)
        }
        if (goneOnCompleteAfterDelay) {
            goneOnCompleteAfterDelay = false
            throw ChunkedUploadGoneException("Fake storage: upload is gone (complete, after delay)")
        }
        // Idempotent and safe under concurrent calls for the same `state` (computeIfAbsent runs
        // the mapping function at most once): a real storage backend's chunkedComplete would
        // presumably behave the same way (same state -> same dataKey) rather than erroring on a
        // second call. This matters for tests: if this method instead removed `state` from
        // `active` and errored on a repeat/concurrent call, two callers racing to complete the
        // same upload would appear serialized by accident *here*, masking whether the caller's
        // own COMPLETING-transition CAS gating (the thing under test) is doing anything at all.
        return completedKeyByState.computeIfAbsent(state) {
            val chunks = active[state] ?: error("Unknown chunked-upload state: '$state'")
            val assembled = ByteArrayOutputStream()
            chunks.entries.sortedBy { it.key }.forEach { (_, bytes) -> assembled.write(bytes) }
            val dataKey = "fake-data/" + UUID.randomUUID()
            completed[dataKey] = assembled.toByteArray()
            dataKey
        }
    }

    override fun chunkedAbort(storageRef: EntityRef, state: String) {
        checkStorageRef(storageRef, "abort")
        maybeThrowGone("abort")
        active.remove(state)
    }
}

/**
 * Stands in the data source context, which is built once before any test of a case runs, and
 * forwards to whatever detector that test installed.
 */
internal class SwappableMimeTypeDetector : ContentMimeTypeDetector {

    @Volatile
    var target: ContentMimeTypeDetector? = null

    private fun target(): ContentMimeTypeDetector {
        return target ?: error("No mime type detector is installed")
    }

    override fun detect(name: String, content: InputStream): MimeType? {
        return target().detect(name, content)
    }

    override fun getPrefixSize(): Int {
        // Delegated when a test installed a detector, so a test can pin what a caller does with the
        // size that detector answers. The fallback is for the calls that reach this holder before
        // any test has installed one - it stands in the data source context, which is built once
        // before the first test of a case runs.
        return target?.getPrefixSize() ?: DEFAULT_PREFIX_SIZE
    }

    companion object {
        private const val DEFAULT_PREFIX_SIZE = 64 * 1024
    }

    override fun getExtension(mimeType: MimeType): String {
        return target().getExtension(mimeType)
    }
}

/**
 * Records every call and answers whatever the test set up, including the contract violations
 * the caller has to survive.
 */
internal class RecordingMimeTypeDetector : ContentMimeTypeDetector {

    val detectCalls = CopyOnWriteArrayList<DetectCall>()
    val extensionCalls = CopyOnWriteArrayList<MimeType>()

    /**
     * Answered from [getPrefixSize]; settable so a test can pin what a caller does with a value the
     * contract forbids.
     */
    @Volatile
    var prefixSizeAnswer: Int = 64 * 1024

    @Volatile
    var result: MimeType? = null

    @Volatile
    var extension: String = ""

    // Throwable rather than Exception: what the caller does with an Error or an
    // InterruptedException differs from what it does with an ordinary failure, so both have to
    // be injectable here.
    @Volatile
    var detectFailure: Throwable? = null

    @Volatile
    var extensionFailure: Throwable? = null

    override fun detect(name: String, content: InputStream): MimeType? {
        // drained before anything else, so a test can check what the caller actually handed over
        detectCalls.add(DetectCall(name, content.readBytes()))
        detectFailure?.let { throw it }
        return result
    }

    override fun getExtension(mimeType: MimeType): String {
        extensionCalls.add(mimeType)
        extensionFailure?.let { throw it }
        return extension
    }

    override fun getPrefixSize(): Int {
        return prefixSizeAnswer
    }

    class DetectCall(val name: String, val content: ByteArray)
}

/**
 * Answers "chunked upload is not supported" from [isChunkedSupported]/[chunkedInit] for every
 * storage ref other than [supportedRef], mirroring the real service's LOCAL/unsupported
 * handling. Every other call is delegated to [fake] whatever the ref, since by then the ref has
 * already been resolved and this harness has only the one storage behind it.
 */
internal class TestChunkedStorageService(
    private val fake: FakeChunkedStorage,
    private val supportedRef: EntityRef
) : EcosContentStorageService {

    override fun resetColumnsCache() {}

    override fun uploadContent(
        storageRef: EntityRef,
        storageConfig: ObjectData,
        action: (OutputStream) -> Unit
    ): String {
        return fake.uploadContent(storageRef, storageConfig, action)
    }

    override fun <T> readContent(storageRef: EntityRef, path: String, range: ContentRange, action: (InputStream) -> T): T {
        return fake.readContent(storageRef, path, range, action)
    }

    override fun deleteContent(storageRef: EntityRef, path: String) {
        fake.deleteContent(storageRef, path)
    }

    override fun isChunkedSupported(storageRef: EntityRef): Boolean {
        return storageRef == supportedRef
    }

    override fun chunkedInit(
        storageRef: EntityRef,
        meta: ChunkedInitMeta
    ): ChunkedInitResult {
        if (storageRef != supportedRef) {
            return ChunkedInitResult(false, "", 0)
        }
        return fake.chunkedInit(storageRef, meta)
    }

    override fun chunkedWriteChunk(
        storageRef: EntityRef,
        state: String,
        chunkIndex: Int,
        content: InputStream,
        contentLength: Long
    ): String {
        return fake.chunkedWriteChunk(storageRef, state, chunkIndex, content, contentLength)
    }

    override fun chunkedComplete(storageRef: EntityRef, state: String): String {
        return fake.chunkedComplete(storageRef, state)
    }

    override fun chunkedAbort(storageRef: EntityRef, state: String) {
        fake.chunkedAbort(storageRef, state)
    }
}
