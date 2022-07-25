package ru.citeck.ecos.data.sql.pg.records

import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.utils.io.IOUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao
import java.util.*

class DbRecordsContentAttTest : DbRecordsTestBase() {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    @Test
    fun test() {

        val contentAttName0 = "contentAtt0"
        val contentAttName1 = "contentAtt1"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId(contentAttName0)
                    withType(AttributeType.CONTENT)
                },
                AttributeDef.create {
                    withId(contentAttName1)
                    withType(AttributeType.CONTENT)
                }
            )
        )

        val textContent = "text-file-sample content\n"
        val contentMimeType = "text/plain"
        val textContentBase64 = Base64.getEncoder().encodeToString(textContent.toByteArray(Charsets.UTF_8))
        val fileName = "text-file-sample.txt"

        val contentAttValue = DataValue.create(
            """
            [
              {
                "storage": "base64",
                "name": "text-file-sample-2b44c2f6-6cd5-458d-ab1e-f7273058f32a.txt",
                "url": "data:$contentMimeType;base64,$textContentBase64",
                "size": 25,
                "type": "$contentMimeType",
                "originalName": "$fileName"
              }
            ]
            """.trimIndent()
        )

        val ref = createRecord(
            contentAttName0 to contentAttValue,
            contentAttName1 to contentAttValue[0]
        )

        val checkContent = { contentAttName: String ->

            val (utf8String, meta) = recordsDao.readContent(ref.id, contentAttName) { meta, input ->
                IOUtils.readAsString(input) to meta
            }

            assertThat(utf8String).isEqualTo(textContent)
            assertThat(meta.mimeType).isEqualTo(contentMimeType)

            val contentDataJson = records.getAtt(ref, "$contentAttName._as.content-data?json")

            val expectedContentSize = textContent.toByteArray(Charsets.UTF_8).size

            assertThat(contentDataJson.size()).isEqualTo(3)
            assertThat(contentDataJson["name"].asText()).isEqualTo(records.getAtt(ref, "?disp").asText())
            assertThat(contentDataJson["size"].asInt()).isEqualTo(expectedContentSize)
            assertThat(contentDataJson["url"].asText()).isNotBlank

            assertThat(records.getAtt(ref, "$contentAttName.size").asInt()).isEqualTo(expectedContentSize)
        }

        listOf(contentAttName0, contentAttName1).forEach {
            try {
                checkContent(it)
                val json = records.getAtt(ref, "$it._as.content-data?json")
                updateRecord(ref, it to json)
                checkContent(it)
            } catch (e: Throwable) {
                log.error { "Attribute: $it" }
                throw e
            }
        }
    }

    @Test
    fun alfContentTest() {

        val textContent = "text-file-sample content\n"
        val contentAttName0 = "contentAtt0"

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(contentAttName0)
                    withType(AttributeType.CONTENT)
                }
            )
        )

        records.register(object : RecordAttsDao {
            override fun getId() = "alfresco/"
            override fun getRecordAtts(recordId: String): Any? {
                if (recordId != "workspace://SpacesStore/d3f2fdb0-0769-488a-bdfd-95a0be0ad100") {
                    return null
                }
                val res = ObjectData.create()
                res["_content"] = ObjectData.create()
                    .set("bytes", textContent.toByteArray(Charsets.UTF_8))
                    .set("mimetype", "image/jpeg")
                return res
            }
        })

        val contentAttValue = DataValue.create(
            """
            [
              {
                "size": 9668,
                "name": "photo.jpeg",
                "data": {
                  "nodeRef": "workspace://SpacesStore/d3f2fdb0-0769-488a-bdfd-95a0be0ad100"
                }
              }
            ]
            """.trimIndent()
        )

        val ref = createRecord(contentAttName0 to contentAttValue)

        val (utf8String, meta) = recordsDao.readContent(ref.id, contentAttName0) { meta, input ->
            IOUtils.readAsString(input) to meta
        }
        assertThat(utf8String).isEqualTo(textContent)
        assertThat(meta.name).isEqualTo("photo.jpeg")

        val bytesFromAttBase64 = records.getAtt(ref, "$contentAttName0.bytes").asText()
        val bytesFromAtt = Base64.getDecoder().decode(bytesFromAttBase64)
        assertThat(String(bytesFromAtt, Charsets.UTF_8)).isEqualTo(textContent)
    }
}
