package ru.citeck.ecos.data.sql.test.content

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.content.EcosContentData

/**
 * Contract test for a byte-range read of a record's content, exercised end to end: from the
 * attribute value returned by [ru.citeck.ecos.data.sql.records.DbRecordsDao.getContent] through
 * [ru.citeck.ecos.data.sql.content.DbContentService] down to the storage which holds the bytes.
 *
 * Runs backend-neutral through [DbRecordsTestBase] (default in-mem; PG/inmem runner subclasses in
 * `ecos-data-sql-pg`/`ecos-data-inmem` just re-trigger this suite under their own package so the
 * modules' `dependenciesToScan` surefire config picks it up - see
 * `InMemContentRangeReadContractTest`/`PgContentRangeReadContractTest`).
 */
open class ContentRangeReadContractTest : DbRecordsTestBase() {

    companion object {
        private const val CONTENT_ATT = "content"
        private const val CONTENT_TEXT = "abcdefghijklmnopqrstuvwxyz"
    }

    private fun createRecordWithContent(): EcosContentData {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(CONTENT_ATT)
                    withType(AttributeType.CONTENT)
                }
            )
        )
        val ref = createRecord(
            CONTENT_ATT to createTempRecord(
                "range-sample.txt",
                MimeTypes.TXT_PLAIN,
                CONTENT_TEXT.toByteArray()
            )
        )
        return recordsDao.getContent(ref.getLocalId(), CONTENT_ATT)
            ?: error("Content is not found for $ref")
    }

    private fun EcosContentData.readRange(range: ContentRange): String {
        return readContent(range) { String(it.readBytes()) }
    }

    @Test
    fun `read ranges of a record content`() {

        val content = createRecordWithContent()

        assertThat(content.getSize()).isEqualTo(CONTENT_TEXT.length.toLong())
        assertThat(content.readContent { String(it.readBytes()) }).isEqualTo(CONTENT_TEXT)

        assertThat(content.readRange(ContentRange(0, 5))).isEqualTo("abcde")
        assertThat(content.readRange(ContentRange(5, 5))).isEqualTo("fghij")
        assertThat(content.readRange(ContentRange.from(20))).isEqualTo("uvwxyz")

        // a range which extends past the end of the content is truncated, not rejected
        assertThat(content.readRange(ContentRange(20, 1000))).isEqualTo("uvwxyz")
        assertThat(content.readRange(ContentRange(0, 1000))).isEqualTo(CONTENT_TEXT)
    }

    @Test
    fun `read the whole content back range by range`() {

        val content = createRecordWithContent()

        val chunkSize = 7L
        val result = StringBuilder()
        var offset = 0L
        while (offset < content.getSize()) {
            result.append(content.readRange(ContentRange(offset, chunkSize)))
            offset += chunkSize
        }

        assertThat(result.toString()).isEqualTo(CONTENT_TEXT)
    }
}
