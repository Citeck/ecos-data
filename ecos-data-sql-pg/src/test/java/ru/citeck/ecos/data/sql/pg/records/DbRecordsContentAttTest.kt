package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.utils.digest.DigestUtils
import ru.citeck.ecos.commons.utils.io.IOUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.*

class DbRecordsContentAttTest : DbRecordsTestBase() {

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

        val checkContent = { contentAttName: String, contentName: String ->

            val contentData = recordsDao.getContent(ref.id, contentAttName)
            val utf8String = contentData?.readContent { IOUtils.readAsString(it) }
            val mimeType = records.getAtt(ref, "$contentAttName.mimeType").asText()

            assertThat(utf8String).isEqualTo(textContent)
            assertThat(mimeType).isEqualTo(contentMimeType)

            val contentDataJson = records.getAtt(ref, "$contentAttName._as.content-data?json")

            val expectedContentSize = textContent.toByteArray(Charsets.UTF_8).size

            val expectedName = if (contentAttName == "_content") {
                records.getAtt(ref, "?disp").asText()
            } else {
                contentName
            }
            val contentNameFromAtt = records.getAtt(ref, "$contentAttName.name").asText()

            assertThat(contentDataJson.size()).isEqualTo(3)
            assertThat(contentDataJson["name"].asText()).isEqualTo(expectedName).isEqualTo(contentNameFromAtt)
            assertThat(contentDataJson["size"].asInt()).isEqualTo(expectedContentSize)
            assertThat(contentDataJson["url"].asText()).isNotBlank

            assertThat(records.getAtt(ref, "$contentAttName.size").asInt()).isEqualTo(expectedContentSize)
        }

        listOf(contentAttName0, contentAttName1).forEach {
            try {
                checkContent(it, fileName)
                val json = records.getAtt(ref, "$it._as.content-data?json")
                updateRecord(ref, it to json)
                checkContent(it, fileName)
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

        val contentData = recordsDao.getContent(ref.id, contentAttName0)
        val utf8String = contentData?.readContent { IOUtils.readAsString(it) }

        val contentName = records.getAtt(ref, "$contentAttName0.name").asText()

        assertThat(utf8String).isEqualTo(textContent)
        assertThat(contentName).isEqualTo("photo.jpeg")

        val bytesFromAttBase64 = records.getAtt(ref, "$contentAttName0.bytes").asText()
        val bytesFromAtt = Base64.getDecoder().decode(bytesFromAttBase64)
        assertThat(String(bytesFromAtt, Charsets.UTF_8)).isEqualTo(textContent)
    }

    @Test
    fun uploadFileTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("content")
                    withType(AttributeType.CONTENT)
                },
                AttributeDef.create {
                    withId("name")
                    withType(AttributeType.MLTEXT)
                }
            )
        )

        val content = "some-text-content".toByteArray()

        val fileRef = RequestContext.doWithCtx {
            recordsDao.uploadFile(
                REC_TEST_TYPE_ID,
                "some-name",
                "plain/text",
                "UTF-8",
                null
            ) {

                it.writeBytes(content)
            }
        }

        val checkContent: (EntityRef, String) -> Unit = { ref, name ->

            log.info { "Content check for ref $ref" }

            val getAtt: (String) -> String = {
                records.getAtt(ref, it).asText()
            }

            assertThat(getAtt("content.mimeType")).isEqualTo("plain/text")
            assertThat(getAtt("content.size").toLong()).isEqualTo(content.size.toLong())
            assertThat(getAtt("content.sha256")).isEqualTo(DigestUtils.getSha256(content).hash)
            assertThat(getAtt("content.encoding")).isEqualTo("UTF-8")
            assertThat(getAtt("content.bytes")).isEqualTo(Base64.getEncoder().encodeToString(content))
            assertThat(getAtt("content.name")).isEqualTo("some-name")
            assertThat(getAtt("?disp")).isEqualTo(name)
        }

        checkContent(fileRef, "some-name")

        val newRecord = createRecord(
            "name" to "CustomName",
            "content" to fileRef
        )

        checkContent(newRecord, "CustomName")
    }

    @Test
    fun uploadWithTempFileTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("content")
                    withType(AttributeType.CONTENT)
                }
            )
        )
        val contentSrc = "abcd".toByteArray()

        RequestContext.doWithCtx {
            val uploadedFile = recordsDao.uploadFile(
                ecosType = REC_TEST_TYPE_ID
            ) { it.writeBytes(contentSrc) }

            val contentFromUploadedFile = records.getAtt(uploadedFile, "_content.bytes").asText()
            assertThat(Base64.getDecoder().decode(contentFromUploadedFile)).isEqualTo(contentSrc)

            val newFile = records.create(
                recordsDao.getId(),
                ObjectData.create()
                    .set("_type", REC_TEST_TYPE_REF)
                    .set("content", uploadedFile)
            )

            val bytesFromAtt = Base64.getDecoder().decode(records.getAtt(newFile, "content.bytes").asText())
            val bytesFromRead = recordsDao.getContent(newFile.id, "_content")?.readContentAsBytes()

            assertThat(bytesFromRead).isEqualTo(bytesFromAtt).isEqualTo(contentSrc)
        }
    }
}
