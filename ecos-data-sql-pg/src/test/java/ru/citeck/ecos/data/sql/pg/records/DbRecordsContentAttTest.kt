package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.utils.io.IOUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.util.*

class DbRecordsContentAttTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId("contentAtt")
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

        val ref = createRecord("contentAtt" to contentAttValue)

        val checkContent = {

            val (utf8String, meta) = recordsDao.readContent(ref.id, "contentAtt") { meta, input ->
                IOUtils.readAsString(input) to meta
            }

            assertThat(utf8String).isEqualTo(textContent)
            assertThat(meta.mimeType).isEqualTo(contentMimeType)

            val contentDataJson = records.getAtt(ref, "contentAtt._as.content-data?json")

            val expectedContentSize = textContent.toByteArray(Charsets.UTF_8).size

            assertThat(contentDataJson.size()).isEqualTo(3)
            assertThat(contentDataJson.get("name").asText()).isEqualTo(records.getAtt(ref, "?disp").asText())
            assertThat(contentDataJson.get("size").asInt()).isEqualTo(expectedContentSize)
            assertThat(contentDataJson.get("url").asText()).isNotBlank

            assertThat(records.getAtt(ref, "contentAtt.size").asInt()).isEqualTo(expectedContentSize)
        }

        checkContent()

        val json = records.getAtt(ref, "contentAtt._as.content-data?json")
        updateRecord(ref, "contentAtt" to json)

        checkContent()
    }
}
