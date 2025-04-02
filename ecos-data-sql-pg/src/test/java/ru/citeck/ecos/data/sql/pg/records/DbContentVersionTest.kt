package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.listener.DbRecordContentChangedEvent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListenerAdapter
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeAspectDef
import ru.citeck.ecos.model.lib.utils.ModelUtils

class DbContentVersionTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("content")
                    withType(AttributeType.CONTENT)
                }
            )
        )

        val contentData0 = createTempRecord(content = "content-0".toByteArray())
        val contentData1 = createTempRecord(content = "content-1".toByteArray())

        val rec0 = createRecord()
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEmpty()
        updateRecord(rec0, "content" to contentData0)
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEmpty()
        updateRecord(rec0, "content" to contentData1)
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEmpty()

        updateType {
            it.withAspects(
                listOf(
                    *it.aspects.toTypedArray(),
                    TypeAspectDef.create {
                        withRef(ModelUtils.getAspectRef(DbRecord.ASPECT_VERSIONABLE))
                    }
                )
            )
        }
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("1.0")
        updateRecord(rec0, "content" to contentData0)
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("2.0")
        updateRecord(rec0, "content" to contentData0)
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("2.0")
        updateRecord(rec0, "content" to contentData1)
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("3.0")
        updateRecord(rec0, "content" to contentData0, "version:version" to "+0.1")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("3.1")
        updateRecord(rec0, "content" to contentData0, "version:version" to "+0.4.1")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("3.5.1")
        updateRecord(rec0, "version:version" to "3.6")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("3.6")

        val error = assertThrows<RuntimeException> {
            updateRecord(rec0, "version:version" to "1.6")
        }
        assertThat(error.message).contains("Version downgrading is not supported")

        assertThrows<RuntimeException> {
            updateRecord(rec0, "version:version" to "abc")
        }

        updateRecord(rec0, "version:version" to "5.0", "version:comment" to "version comment")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION_COMMENT).asText()).isEqualTo("version comment")
        updateRecord(rec0, "version:version" to "5.1")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION_COMMENT).asText()).isEmpty()
        updateRecord(rec0, "version:version" to "5.2", "version:comment" to "version comment")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION_COMMENT).asText()).isEqualTo("version comment")
        updateRecord(rec0, "content" to contentData1)
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION_COMMENT).asText()).isEqualTo("")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("6.0")
        updateRecord(rec0, "version:version" to "+1.1")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("7.1")
        updateRecord(rec0, "version:version" to "+0.1")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("7.2")
        updateRecord(rec0, "version:version" to "+1")
        assertThat(records.getAtt(rec0, DbRecord.ATT_CONTENT_VERSION).asText()).isEqualTo("8.0")

        val contentChangedEvents = mutableListOf<DbRecordContentChangedEvent>()
        mainCtx.dao.addListener(object : DbRecordsListenerAdapter() {
            override fun onContentChanged(event: DbRecordContentChangedEvent) {
                contentChangedEvents.add(event)
            }
        })

        updateRecord(rec0, "content" to contentData0, DbRecord.ATT_CONTENT_VERSION_COMMENT to "comment")
        assertThat(contentChangedEvents).hasSize(1)
        assertThat(contentChangedEvents[0].attsBefore[DbRecord.ATT_CONTENT_VERSION]).isEqualTo("8.0")
        assertThat(contentChangedEvents[0].attsBefore[DbRecord.ATT_CONTENT_VERSION_COMMENT]).isEqualTo("")
        assertThat(contentChangedEvents[0].attsAfter[DbRecord.ATT_CONTENT_VERSION]).isEqualTo("9.0")
        assertThat(contentChangedEvents[0].attsAfter[DbRecord.ATT_CONTENT_VERSION_COMMENT]).isEqualTo("comment")
    }
}
