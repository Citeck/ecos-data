package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.storage.ChunkedInitMeta
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.records.dao.atts.content.DbDefaultLocalContentValue
import ru.citeck.ecos.data.sql.records.dao.atts.content.HasEcosContentDbData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Pins that a range read of a record's content is handed down to the content storage service
 * instead of being emulated on top of a full read somewhere in the middle of the chain. Both
 * produce the same bytes, so only the storage service itself can tell them apart - hence the
 * recording service installed through [DataMockFactory.contentStorageServiceOverride].
 */
class DbRecordsContentRangeTest : DbRecordsTestBase() {

    companion object {
        private const val CONTENT_ATT = "content"
        private const val CONTENT_TEXT = "abcdefghijklmnopqrstuvwxyz"
    }

    private val storage = RecordingRangeStorageService()

    init {
        contentStorageServiceOverride = storage
    }

    private fun createRecordWithContent(): EntityRef {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(CONTENT_ATT)
                    withType(AttributeType.CONTENT)
                }
            )
        )
        return createRecord(
            CONTENT_ATT to ContentUtils.createContentObjFromText(CONTENT_TEXT)
        )
    }

    @Test
    fun `a range read reaches the storage service as a range, a plain read reaches it without one`() {
        val ref = createRecordWithContent()
        assertRangesReachStorage(
            recordsDao.getContent(ref.getLocalId(), CONTENT_ATT) ?: error("Content is not found for $ref")
        )
    }

    /**
     * The record's own `_content` attribute is served by [DbDefaultLocalContentValue], which wraps
     * the content data in a delegate of its own. It is the only way to obtain that wrapper -
     * [ru.citeck.ecos.data.sql.records.dao.content.DbRecordsContentDao.getContent] resolves
     * `_content` to the concrete attribute name and hands out a plain
     * [ru.citeck.ecos.data.sql.records.dao.atts.content.DbContentValue] - so the wrapper is reached
     * through the record value here.
     */
    @Test
    fun `a range read of the default content attribute value reaches the storage service as a range`() {

        val ref = createRecordWithContent()

        val recordValue = recordsDao.getRecordsAtts(listOf(ref.getLocalId())).first()
        recordValue.init()
        val contentValue = recordValue.getAtt(RecordConstants.ATT_CONTENT)

        assertThat(contentValue).isInstanceOf(DbDefaultLocalContentValue::class.java)
        assertRangesReachStorage((contentValue as HasEcosContentDbData).getContentDbData())
    }

    private fun assertRangesReachStorage(content: EcosContentData) {

        storage.readRanges.clear()

        assertThat(content.readContent(ContentRange(5, 5)) { String(it.readBytes()) }).isEqualTo("fghij")
        assertThat(storage.readRanges).containsExactly(ContentRange(5, 5))

        storage.readRanges.clear()

        assertThat(content.readContent(ContentRange.from(20)) { String(it.readBytes()) }).isEqualTo("uvwxyz")
        assertThat(storage.readRanges).containsExactly(ContentRange.from(20))

        storage.readRanges.clear()

        assertThat(content.readContent { String(it.readBytes()) }).isEqualTo(CONTENT_TEXT)
        assertThat(storage.readRanges).containsExactly(ContentRange.UNBOUNDED)
    }

    /**
     * Keeps every uploaded blob in memory and records the range of every read. The plain read
     * overload is expressed through the range one so that a null range is recorded too.
     */
    private class RecordingRangeStorageService : EcosContentStorageService {

        val readRanges: MutableList<ContentRange?> = ArrayList()

        private val blobs = ConcurrentHashMap<String, ByteArray>()
        private val keyCounter = AtomicLong()

        override fun resetColumnsCache() {}

        override fun uploadContent(
            storageRef: EntityRef,
            storageConfig: ObjectData,
            action: (OutputStream) -> Unit
        ): String {
            val output = ByteArrayOutputStream()
            action(output)
            val dataKey = "recorded-" + keyCounter.incrementAndGet()
            blobs[dataKey] = output.toByteArray()
            return dataKey
        }

        override fun <T> readContent(
            storageRef: EntityRef,
            path: String,
            range: ContentRange,
            action: (InputStream) -> T
        ): T {
            readRanges.add(range)
            val data = blobs[path] ?: error("Content doesn't exists for key: $path")
            if (range.isUnbounded()) {
                return action(ByteArrayInputStream(data))
            }
            val offset = minOf(range.offset, data.size.toLong())
            val available = data.size - offset
            val length = if (range.length == ContentRange.LENGTH_TO_END) {
                available
            } else {
                minOf(range.length, available)
            }
            return action(ByteArrayInputStream(data, offset.toInt(), length.toInt()))
        }

        override fun deleteContent(storageRef: EntityRef, path: String) {
            blobs.remove(path)
        }

        override fun isChunkedSupported(storageRef: EntityRef): Boolean = TODO("Not yet implemented")

        override fun chunkedInit(
            storageRef: EntityRef,
            meta: ChunkedInitMeta
        ) = TODO("Not yet implemented")

        override fun chunkedWriteChunk(
            storageRef: EntityRef,
            state: String,
            chunkIndex: Int,
            content: InputStream,
            contentLength: Long
        ) = TODO("Not yet implemented")

        override fun chunkedComplete(
            storageRef: EntityRef,
            state: String
        ) = TODO("Not yet implemented")

        override fun chunkedAbort(
            storageRef: EntityRef,
            state: String
        ) = TODO("Not yet implemented")
    }
}
