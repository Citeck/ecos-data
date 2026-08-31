package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.commons.utils.digest.DigestUtils
import ru.citeck.ecos.commons.utils.io.IOUtils
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeContentConfig
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.dao.atts.RecordAttsDao
import ru.citeck.ecos.records3.record.request.RequestContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import ru.citeck.ecos.webapp.api.mime.MimeType
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

            val contentData = recordsDao.getContent(ref.getLocalId(), contentAttName)
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

            assertThat(contentDataJson.size()).isEqualTo(5)
            assertThat(contentDataJson["name"].asText()).isEqualTo(expectedName).isEqualTo(contentNameFromAtt)
            assertThat(contentDataJson["size"].asInt()).isEqualTo(expectedContentSize)
            assertThat(contentDataJson["url"].asText()).isNotBlank
            assertThat(contentDataJson["recordRef"].toEntityRef()).isEqualTo(ref)
            assertThat(contentDataJson["fileType"].asText()).isEqualTo(REC_TEST_TYPE_ID)

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

        val contentData = recordsDao.getContent(ref.getLocalId(), contentAttName0)
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
                    .set("_content", uploadedFile)
            )

            val bytesFromAtt = Base64.getDecoder().decode(records.getAtt(newFile, "content.bytes").asText())
            val bytesFromRead = recordsDao.getContent(newFile.getLocalId(), "_content")?.readContentAsBytes()

            assertThat(bytesFromRead).isEqualTo(bytesFromAtt).isEqualTo(contentSrc)
        }
    }

    @Test
    fun customContentConfigTest() {
        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("name")
                                    .build(),
                                AttributeDef.create()
                                    .withId("systemDocumentLink")
                                    .withType(AttributeType.ASSOC)
                                    .build()
                            )
                        ).build()
                ).build()
                withContentConfig(
                    TypeContentConfig.create()
                        .withPath("systemDocumentLink.content")
                        .withPreviewPath("systemDocumentLink.content")
                        .build()
                )
            }
        )

        val fileName = "text-file-sample.txt"
        val textContent = "text-file-sample content\n"
        val baseName = "base-name.txt"

        val contentRecord = createTempRecord(fileName, MimeTypes.TXT_PLAIN, textContent.toByteArray())
        val record = createRecord("name" to baseName, "systemDocumentLink" to contentRecord)

        val content = String(
            Base64.getDecoder().decode(
                records.getAtt(
                    record,
                    RecordConstants.ATT_CONTENT + ".bytes"
                ).asText()
            )
        )
        val contentFromDao = recordsDao.getContent(record.getLocalId(), RecordConstants.ATT_CONTENT)
        val contentDataFromDao = contentFromDao?.readContent { IOUtils.readAsString(it) }

        val mimeType = records.getAtt(record, "${RecordConstants.ATT_CONTENT}.mimeType").asText()

        assertThat(content).isEqualTo(textContent).isEqualTo(contentDataFromDao)
        assertThat(mimeType).isEqualTo(MimeTypes.TXT_PLAIN.toString())

        val contentDataJson = records.getAtt(record, "${RecordConstants.ATT_CONTENT}._as.content-data?json")
        val contentDataJson2 = records.getAtt(record, "_as.content-data?json")
        assertThat(contentDataJson)
            .isEqualTo(contentDataJson2)

        listOf("url", "name", "size", "recordRef", "fileType").forEach {
            val att = it + when (it) {
                "recordRef" -> "?id"
                else -> "?str"
            }
            assertThat(records.getAtt(record, "_as.content-data.$att").asText())
                .isEqualTo(contentDataJson[it].asText())
        }

        val contentNameFromAtt = records.getAtt(record, "${RecordConstants.ATT_CONTENT}.name").asText()
        val expectedContentSize = textContent.toByteArray(Charsets.UTF_8).size

        assertThat(contentDataJson.size()).isEqualTo(5)
        assertThat(contentDataJson["name"].asText()).isEqualTo(baseName).isEqualTo(contentNameFromAtt)
        assertThat(contentDataJson["size"].asInt()).isEqualTo(expectedContentSize)
        assertThat(contentDataJson["url"].asText()).isNotBlank
        assertThat(contentDataJson["recordRef"].toEntityRef()).isEqualTo(record)
        assertThat(contentDataJson["fileType"].asText()).isEqualTo(REC_TEST_TYPE_ID)
        assertThat(records.getAtt(record, "${RecordConstants.ATT_CONTENT}.size").asInt()).isEqualTo(expectedContentSize)

        val contentJson = records.getAtt(record, RecordConstants.ATT_CONTENT + "?json")

        assertThat(contentJson["name"].asText()).isEqualTo(baseName)
        assertThat(contentJson["extension"]).isEqualTo(records.getAtt(contentRecord, "_content.extension"))
        assertThat(contentJson["sha256"]).isEqualTo(records.getAtt(contentRecord, "_content.sha256"))
        assertThat(contentJson["size"].asText()).isEqualTo(records.getAtt(contentRecord, "_content.size").asText())
        assertThat(contentJson["mimeType"]).isEqualTo(records.getAtt(contentRecord, "_content.mimeType"))
        assertThat(contentJson["encoding"]).isEqualTo(records.getAtt(contentRecord, "_content.encoding"))
        assertThat(contentJson["created"]).isEqualTo(records.getAtt(contentRecord, "_content.created"))
        assertThat(contentJson["creator"]).isEqualTo(records.getAtt(contentRecord, "_content.creator?localId"))
        assertThat(contentJson["url"]).isEqualTo(contentDataJson["url"])

        thumbnailCtx.createRecord(
            RecordConstants.ATT_PARENT to contentRecord,
            RecordConstants.ATT_PARENT_ATT to "thumbnail:thumbnails",
            "mimeType" to MimeTypes.APP_PDF_TEXT,
            "srcAttribute" to "_content",
            "content" to createTempRecord("preview.pdf", MimeTypes.APP_PDF, textContent.toByteArray())
        )

        val expectedPreviewInfo = records.getAtt(contentRecord, "previewInfo?json")
        assertThat(expectedPreviewInfo).isNotEmpty

        assertThat(records.getAtt(contentRecord, "_content.previewInfo?json")).isEqualTo(expectedPreviewInfo)

        assertThat(records.getAtt(record, "previewInfo?json")).isEqualTo(expectedPreviewInfo)
        assertThat(records.getAtt(record, "_content.previewInfo?json")).isEqualTo(expectedPreviewInfo)

        assertThat(records.getAtt(contentRecord, "_has._content?bool").asBoolean()).isTrue()
        assertThat(records.getAtt(record, "_has._content?bool").asBoolean()).isTrue()
    }

    @Test
    fun defaultContentUrlRoundTripTest() {
        val contentAttName = "content"
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(contentAttName)
                    withType(AttributeType.CONTENT)
                }
            )
        )

        val textContent = "text-file-sample content\n"
        val fileName = "text-file-sample.txt"

        val ref = createRecord(
            contentAttName to createTempRecord(fileName, MimeTypes.TXT_PLAIN, textContent.toByteArray())
        )

        val readContent = { recordsDao.getContent(ref.getLocalId(), contentAttName)?.readContent { IOUtils.readAsString(it) } }
        assertThat(readContent()).isEqualTo(textContent)

        // content-data URL of the default content is built against `_content`
        val contentDataJson = records.getAtt(ref, "${RecordConstants.ATT_CONTENT}._as.content-data?json")
        assertThat(contentDataJson["url"].asText()).contains("att=${RecordConstants.ATT_CONTENT}")

        // simulate "save without re-uploading the file": round-trip the same content-data back
        updateRecord(ref, RecordConstants.ATT_CONTENT to contentDataJson)

        // content must be preserved, not erased
        assertThat(readContent()).isEqualTo(textContent)
        assertThat(records.getAtt(ref, "$contentAttName.size").asInt())
            .isEqualTo(textContent.toByteArray(Charsets.UTF_8).size)
    }

    private fun registerContentAtts(vararg names: String) {
        registerAtts(
            names.map { name ->
                AttributeDef.create {
                    withId(name)
                    withType(AttributeType.CONTENT)
                }
            }
        )
    }

    private fun assertOriginalIsDescribed(previewInfo: DataValue, mimeType: MimeType, ext: String, size: Int) {
        assertThat(previewInfo["originalUrl"].asText()).isNotBlank
        assertThat(previewInfo["originalName"].asText()).isEqualTo("sample.$ext")
        assertThat(previewInfo["originalExt"].asText()).isEqualTo(ext)
        assertThat(previewInfo["originalMimeType"].asText()).isEqualTo(mimeType.toString())
        assertThat(previewInfo["originalSize"].asLong()).isEqualTo(size.toLong())
    }

    private fun assertNothingToRender(previewInfo: DataValue, status: String) {
        assertThat(previewInfo["kind"].asText()).isEqualTo("none")
        assertThat(previewInfo["status"].asText()).isEqualTo(status)
        assertThat(previewInfo["url"].asText()).isEmpty()
        assertThat(previewInfo["ext"].asText()).isEmpty()
        assertThat(previewInfo["mimetype"].asText()).isEmpty()
        assertThat(previewInfo["mimeType"].asText()).isEmpty()
        assertThat(previewInfo["size"].asLong()).isEqualTo(0L)
    }

    /**
     * The content is renderable as it is, so the preview is the original itself.
     */
    @Test
    fun previewInfoForNativelyRenderableContentTest() {

        val contentAttName = "content"
        registerContentAtts(contentAttName)

        val checkNativePreview: (MimeType, String, String) -> Unit = { mimeType, ext, kind ->

            val content = "content".toByteArray()
            val rec = createRecord(
                contentAttName to createTempRecord("sample.$ext", mimeType, content)
            )

            val previewInfo = records.getAtt(rec, "$contentAttName.previewInfo?json")

            assertThat(previewInfo.isObject()).isTrue()
            assertThat(previewInfo["kind"].asText()).isEqualTo(kind)
            assertThat(previewInfo["status"].asText()).isEqualTo("ready")
            assertThat(previewInfo["url"].asText()).isEqualTo(previewInfo["originalUrl"].asText())
            assertThat(previewInfo["ext"].asText()).isEqualTo(ext)
            assertThat(previewInfo["mimetype"].asText())
                .isEqualTo(previewInfo["mimeType"].asText())
                .isEqualTo(mimeType.toString())
            assertThat(previewInfo["size"].asLong()).isEqualTo(previewInfo["originalSize"].asLong())

            assertOriginalIsDescribed(previewInfo, mimeType, ext, content.size)
        }

        checkNativePreview(MimeTypes.TXT_PLAIN, "txt", "text")
        checkNativePreview(MimeTypes.TXT_CSV, "csv", "text")
        checkNativePreview(MimeTypes.TXT_HTML, "html", "text")
        checkNativePreview(MimeTypes.APP_JSON, "json", "text")
        checkNativePreview(MimeTypes.APP_XML, "xml", "text")
        checkNativePreview(MimeTypes.APP_YAML, "yaml", "text")
        checkNativePreview(MimeTypes.TXT_MARKDOWN, "md", "markdown")
        checkNativePreview(MimeTypes.IMG_PNG, "png", "image")
        checkNativePreview(MimeTypes.APP_PDF, "pdf", "pdf")
        checkNativePreview(MimeTypes.VIDEO_MP4, "mp4", "video")
        checkNativePreview(MimeTypes.AUDIO_MPEG, "mp3", "audio")
    }

    /**
     * The content needs a converter, so the preview is the generated thumbnail and the thumbnail's
     * own status becomes the status of the preview.
     */
    @Test
    fun previewInfoFromThumbnailTest() {

        val contentAttName = "content"
        registerContentAtts(contentAttName)

        val docxContent = "docx-content".toByteArray()
        val previewContent = "pdf-preview-content".toByteArray()

        val createDocxRecord = {
            createRecord(
                contentAttName to createTempRecord("sample.docx", MimeTypes.APP_DOCX, docxContent)
            )
        }
        val createThumbnail: (EntityRef, String, EntityRef) -> Unit = { rec, status, content ->
            thumbnailCtx.createRecord(
                RecordConstants.ATT_PARENT to rec,
                RecordConstants.ATT_PARENT_ATT to "thumbnail:thumbnails",
                "mimeType" to MimeTypes.APP_PDF_TEXT,
                "srcAttribute" to RecordConstants.ATT_CONTENT,
                "status" to status,
                "content" to content
            )
        }
        val previewContentRecord = {
            createTempRecord("preview.pdf", MimeTypes.APP_PDF, previewContent)
        }

        val readyRec = createDocxRecord()
        createThumbnail(readyRec, "PROCESSED", previewContentRecord())

        val readyInfo = records.getAtt(readyRec, "$contentAttName.previewInfo?json")

        assertThat(readyInfo["kind"].asText()).isEqualTo("pdf")
        assertThat(readyInfo["status"].asText()).isEqualTo("ready")
        assertThat(readyInfo["url"].asText()).isNotBlank
        assertThat(readyInfo["url"].asText()).isNotEqualTo(readyInfo["originalUrl"].asText())
        assertThat(readyInfo["ext"].asText()).isEqualTo("pdf")
        assertThat(readyInfo["mimetype"].asText())
            .isEqualTo(readyInfo["mimeType"].asText())
            .isEqualTo(MimeTypes.APP_PDF_TEXT)
        assertThat(readyInfo["size"].asLong()).isEqualTo(previewContent.size.toLong())
        assertOriginalIsDescribed(readyInfo, MimeTypes.APP_DOCX, "docx", docxContent.size)

        listOf(
            "DRAFT" to "processing",
            "PROCESSING" to "processing",
            "SOMETHING_NEW" to "processing",
            "FAILED" to "failed"
        ).forEach { (thumbnailStatus, expectedStatus) ->

            val rec = createDocxRecord()
            createThumbnail(rec, thumbnailStatus, previewContentRecord())

            val previewInfo = records.getAtt(rec, "$contentAttName.previewInfo?json")

            assertNothingToRender(previewInfo, expectedStatus)
            assertOriginalIsDescribed(previewInfo, MimeTypes.APP_DOCX, "docx", docxContent.size)
        }

        val emptyThumbnailRec = createDocxRecord()
        createThumbnail(emptyThumbnailRec, "PROCESSED", EntityRef.EMPTY)

        val emptyThumbnailInfo = records.getAtt(emptyThumbnailRec, "$contentAttName.previewInfo?json")

        assertNothingToRender(emptyThumbnailInfo, "failed")
        assertOriginalIsDescribed(emptyThumbnailInfo, MimeTypes.APP_DOCX, "docx", docxContent.size)
    }

    /**
     * A converter may store bytes whose mime type differs from the one the thumbnail record declares.
     * The lookup matches on the declared mime type, so such a thumbnail is found, and what decides the
     * outcome is the mime type of the stored bytes: nothing renderable means nothing to render.
     */
    @Test
    fun previewInfoFromThumbnailWithUnrenderableContentMimeTest() {

        val contentAttName = "content"
        registerContentAtts(contentAttName)

        val docxContent = "docx-content".toByteArray()
        val rec = createRecord(
            contentAttName to createTempRecord("sample.docx", MimeTypes.APP_DOCX, docxContent)
        )

        thumbnailCtx.createRecord(
            RecordConstants.ATT_PARENT to rec,
            RecordConstants.ATT_PARENT_ATT to "thumbnail:thumbnails",
            "mimeType" to MimeTypes.APP_PDF_TEXT,
            "srcAttribute" to RecordConstants.ATT_CONTENT,
            "status" to "PROCESSED",
            "content" to createTempRecord("preview.pdf", MimeTypes.APP_BIN, "not-a-pdf".toByteArray())
        )

        val previewInfo = records.getAtt(rec, "$contentAttName.previewInfo?json")

        assertNothingToRender(previewInfo, "failed")
        assertOriginalIsDescribed(previewInfo, MimeTypes.APP_DOCX, "docx", docxContent.size)
    }

    /**
     * The thumbnail lookup is keyed by the source attribute the thumbnail was generated from: a
     * thumbnail naming one content attribute is not offered as the preview of another one.
     *
     * It also only runs for the default content attribute. The lookup is one query per content
     * value and previewInfo is requested over whole lists of records, so running it for every
     * content attribute would multiply that N+1 by the number of attributes; a named attribute
     * therefore reports `unsupported` even when a thumbnail naming it exists.
     */
    @Test
    fun previewInfoThumbnailLookupIsKeyedBySrcAttributeTest() {

        val defaultContentAtt = "content"
        val namedContentAtt = "secondContent"
        registerContentAtts(defaultContentAtt, namedContentAtt)

        val docxContent = "docx-content".toByteArray()

        val rec = createRecord(
            defaultContentAtt to createTempRecord("sample.docx", MimeTypes.APP_DOCX, docxContent),
            namedContentAtt to createTempRecord("sample.docx", MimeTypes.APP_DOCX, docxContent)
        )

        fun createThumbnail(srcAttribute: String) {
            thumbnailCtx.createRecord(
                RecordConstants.ATT_PARENT to rec,
                RecordConstants.ATT_PARENT_ATT to "thumbnail:thumbnails",
                "mimeType" to MimeTypes.APP_PDF_TEXT,
                "srcAttribute" to srcAttribute,
                "status" to "PROCESSED",
                "content" to createTempRecord("preview.pdf", MimeTypes.APP_PDF, "pdf".toByteArray())
            )
        }

        createThumbnail(namedContentAtt)

        // the thumbnail of a named attribute is not offered as the preview of the default one
        val defaultInfoBefore = records.getAtt(rec, "$defaultContentAtt.previewInfo?json")
        assertNothingToRender(defaultInfoBefore, "unsupported")
        assertOriginalIsDescribed(defaultInfoBefore, MimeTypes.APP_DOCX, "docx", docxContent.size)

        // nor is it offered as the preview of the attribute it does name: only the default content
        // attribute is looked up at all
        val namedInfo = records.getAtt(rec, "$namedContentAtt.previewInfo?json")
        assertNothingToRender(namedInfo, "unsupported")
        assertOriginalIsDescribed(namedInfo, MimeTypes.APP_DOCX, "docx", docxContent.size)

        // the thumbnail naming the default attribute is - automatic thumbnail creation records it
        // as RecordConstants.ATT_CONTENT, which is the url attribute of the default content value
        createThumbnail(RecordConstants.ATT_CONTENT)

        val defaultInfo = records.getAtt(rec, "$defaultContentAtt.previewInfo?json")
        assertThat(defaultInfo["kind"].asText()).isEqualTo("pdf")
        assertThat(defaultInfo["status"].asText()).isEqualTo("ready")
        assertThat(defaultInfo["url"].asText()).isNotBlank
    }

    /**
     * No converter is declared for the mime type, so no thumbnail record was ever created and there
     * is nothing to wait for.
     */
    @Test
    fun previewInfoWithoutThumbnailRecordTest() {

        val contentAttName = "content"
        registerContentAtts(contentAttName)

        val content = "zip-content".toByteArray()
        val rec = createRecord(
            contentAttName to createTempRecord("sample.zip", MimeTypes.APP_ZIP, content)
        )

        val previewInfo = records.getAtt(rec, "$contentAttName.previewInfo?json")

        assertThat(previewInfo.isObject()).isTrue()
        assertNothingToRender(previewInfo, "unsupported")
        assertOriginalIsDescribed(previewInfo, MimeTypes.APP_ZIP, "zip", content.size)
    }

    /**
     * The default content attribute is served by a wrapper which replaces `originalName` with the
     * entity display name, and the preview info object it returns carries that name too.
     */
    @Test
    fun previewInfoOfDefaultContentAttributeTest() {

        val contentAttName = "content"
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(contentAttName)
                    withType(AttributeType.CONTENT)
                },
                AttributeDef.create {
                    withId("name")
                    withType(AttributeType.MLTEXT)
                }
            )
        )

        val content = "zip-content".toByteArray()
        val rec = createRecord(
            "name" to "display-name",
            contentAttName to createTempRecord("sample.zip", MimeTypes.APP_ZIP, content)
        )

        val previewInfo = records.getAtt(rec, "${RecordConstants.ATT_CONTENT}.previewInfo?json")

        assertNothingToRender(previewInfo, "unsupported")
        assertThat(previewInfo["originalName"].asText()).isEqualTo("display-name.zip")
        assertThat(previewInfo["originalExt"].asText()).isEqualTo("zip")
        assertThat(previewInfo["originalMimeType"].asText()).isEqualTo(MimeTypes.APP_ZIP.toString())
        assertThat(previewInfo["originalSize"].asLong()).isEqualTo(content.size.toLong())
        assertThat(previewInfo["originalUrl"].asText()).isNotBlank
    }
}
