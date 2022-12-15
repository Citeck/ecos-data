package ru.citeck.ecos.data.sql.records.dao.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.utils.DataUriUtil
import ru.citeck.ecos.data.sql.content.DbContentUploadData
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.ByteArrayInputStream
import java.net.URLDecoder
import java.net.URLEncoder

class DbRecContentHandler(private val ctx: DbRecordsDaoCtx) {

    companion object {
        private val contentDbDataAwareSchema = ThreadLocal.withInitial { "" }
    }

    fun <T> withContentDbDataAware(action: () -> T): T {
        val valueBefore = contentDbDataAwareSchema.get()
        contentDbDataAwareSchema.set(ctx.tableRef.schema)
        try {
            return action.invoke()
        } finally {
            contentDbDataAwareSchema.set(valueBefore)
        }
    }

    fun isContentDbDataAware(): Boolean {
        return contentDbDataAwareSchema.get() == ctx.tableRef.schema
    }

    fun createContentUrl(recordId: String, attribute: String): String {

        val ref = ctx.sourceId + EntityRef.SOURCE_ID_DELIMITER + recordId

        val refEnc = URLEncoder.encode(ref, Charsets.UTF_8.name())
        val attEnc = URLEncoder.encode(attribute, Charsets.UTF_8.name())

        return "/gateway/${ctx.appName}/api/ecos/webapp/content?ref=$refEnc&att=$attEnc"
    }

    fun getRecordRefFromContentUrl(url: String): EntityRef {
        if (url.isBlank()) {
            return EntityRef.EMPTY
        }
        val parts = url.split("/")
        if (parts.size != 7) {
            error("Unexpected URL parts size: ${parts.size}. Url: " + url)
        }
        val appName = parts[2]
        val recordsDaoId = URLDecoder.decode(parts[5], Charsets.UTF_8.name())
        val recId = URLDecoder.decode(parts[6], Charsets.UTF_8.name())

        if (recId.isNotBlank()) {
            return EntityRef.create(appName, recordsDaoId, recId)
        }
        return EntityRef.EMPTY
    }

    fun getRefForContentData(value: DataValue): EntityRef {

        val url = value["url"].asText()
        if (url.isBlank()) {
            return EntityRef.EMPTY
        }
        if (url.startsWith("/share/page/card-details")) {
            val nodeRef = value["data"]["nodeRef"].asText()
            if (nodeRef.isNotBlank()) {
                return EntityRef.create("alfresco", "", nodeRef)
            }
        } else if (url.startsWith("/gateway")) {
            return getRecordRefFromContentUrl(url)
        }
        return EntityRef.EMPTY
    }

    fun uploadContent(data: RecordFileUploadData, storageType: String): Long? {

        val contentService = ctx.contentService ?: return null

        return contentService.uploadContent(
            storageType.ifBlank { EcosContentLocalStorage.TYPE },
            DbContentUploadData.create()
                .withEncoding(data.encoding)
                .withName(data.name)
                .withMimeType(data.mimeType)
                .withContent(data.content)
                .build()
        ).getDbId()
    }

    fun uploadContent(
        record: DbEntity,
        attribute: String,
        contentData: DataValue,
        multiple: Boolean,
        storageType: String
    ): Any? {

        val contentService = ctx.contentService ?: return null

        if (contentData.isArray()) {
            if (contentData.size() == 0) {
                return null
            }
            return if (multiple) {
                val newArray = DataValue.createArr()
                contentData.forEach {
                    val contentId = uploadContent(record, attribute, it, false, storageType)
                    if (contentId != null) {
                        newArray.add(contentId)
                    }
                }
                newArray
            } else {
                uploadContent(record, attribute, contentData[0], false, storageType)
            }
        }
        if (!contentData.isObject()) {
            if (contentData.isLong()) {
                if (isContentDbDataAware()) {
                    return contentData.asLong()
                }
            } else if (contentData.isTextual()) {
                val contentText = contentData.asText()
                val appNameDelimIdx = contentText.indexOf(EntityRef.APP_NAME_DELIMITER)
                val srcIdDelimIdx = contentText.indexOf(EntityRef.SOURCE_ID_DELIMITER)
                if (srcIdDelimIdx != -1 && appNameDelimIdx < srcIdDelimIdx) {
                    return uploadFromEntity(EntityRef.valueOf(contentText), storageType)
                }
            }
            return null
        }
        val urlData = contentData["url"]
        var dataBytes: ByteArray? = null
        val uploadData = DbContentUploadData.create()

        if (urlData.isTextual() && urlData.isNotEmpty()) {

            val recordContentUrl = createContentUrl(record.extId, attribute)

            if (urlData.asText() == recordContentUrl) {
                return record.attributes[attribute]
            }
            if (urlData.asText().startsWith(DataUriUtil.DATA_PREFIX)) {
                val data = DataUriUtil.parseData(urlData.asText())
                dataBytes = data.data
                if (dataBytes == null || dataBytes.isEmpty()) {
                    return null
                }
                uploadData.withMimeType(data.mimeType)
                    .withName(contentData["originalName"].asText())
            }
        }
        if (dataBytes == null && contentData.has("data")) {
            val data = contentData["data"]
            if (!data.isObject()) {
                return null
            }
            val name = contentData["originalName"].asText().ifBlank { contentData["name"].asText() }
            val entityRef = data["entityRef"].asText()
            if (entityRef.isNotBlank()) {
                return uploadFromEntity(EntityRef.valueOf(entityRef), storageType)
            } else {
                val nodeRef = data["nodeRef"].asText()
                if (nodeRef.isNotBlank()) {
                    val ref = EntityRef.create(AppName.ALFRESCO, "", nodeRef)
                    val content = ctx.recordsService.getAtts(ref, AlfContentAtts::class.java)
                    dataBytes = content.bytes
                    uploadData.withMimeType(content.mimetype ?: "application/octet-stream")
                        .withName(name.ifBlank { content.name ?: "" })
                } else {
                    return null
                }
            }
        }
        if (dataBytes == null) {
            return null
        }
        uploadData.withContent(ByteArrayInputStream(dataBytes))
        return contentService.uploadContent(
            storageType.ifBlank { EcosContentLocalStorage.TYPE },
            uploadData.build()
        ).getDbId()
    }

    private fun uploadFromEntity(entityRef: EntityRef, storageType: String): Long? {

        val contentService = ctx.contentService ?: return null

        if (entityRef.getAppName() == ctx.appName) {
            val atts = withContentDbDataAware {
                ctx.recordsService.getAtts(entityRef, ContentLocationAtts::class.java)
            }
            if (atts.dbSchema == ctx.tableRef.schema && (atts.contentId ?: -1) >= 0) {
                return atts.contentId
            }
        }
        ctx.contentApi ?: error("Content API is null")
        val metaAtts = ctx.recordsService.getAtts(entityRef, ContentMetaAtts::class.java)
        return if (metaAtts.mimeType.isBlank()) {
            null
        } else {
            ctx.contentApi.getContent(entityRef)?.readContent { input ->
                contentService.uploadContent(
                    storageType.ifBlank { EcosContentLocalStorage.TYPE },
                    DbContentUploadData.create()
                        .withMimeType(metaAtts.mimeType)
                        .withName(metaAtts.name)
                        .withEncoding(metaAtts.encoding)
                        .withContent(input)
                        .build()
                ).getDbId()
            }
        }
    }

    private class AlfContentAtts(
        @AttName("?disp")
        val name: String?,
        @AttName("_content.bytes")
        val bytes: ByteArray?,
        @AttName("_content.mimetype")
        val mimetype: String?
    )

    private class ContentLocationAtts(
        @AttName("_content.tableRef.schema!''")
        val dbSchema: String,
        @AttName("_content.contentDbId?num")
        val contentId: Long?
    )

    private class ContentMetaAtts(
        @AttName("_content.name!")
        val name: String,
        @AttName("_content.mimeType!")
        val mimeType: String,
        @AttName("_content.encoding!")
        val encoding: String
    )
}
