package ru.citeck.ecos.data.sql.records.dao.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.utils.DataUriUtil
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef
import java.net.URLDecoder
import java.net.URLEncoder

class DbRecContentHandler(private val ctx: DbRecordsDaoCtx) {

    companion object {
        private val contentDbDataAwareSchema = ThreadLocal.withInitial { "" }

        private const val CONTENT_URL_PATH_GATEWAY_PART = "/gateway/"
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
        return createContentUrl(EntityRef.create(ctx.appName, ctx.sourceId, recordId), attribute)
    }

    fun createContentUrl(entityRef: EntityRef, attribute: String): String {

        val ref = entityRef.getSourceId() + EntityRef.SOURCE_ID_DELIMITER + entityRef.getLocalId()

        val refEnc = URLEncoder.encode(ref, Charsets.UTF_8.name())
        val attEnc = URLEncoder.encode(attribute, Charsets.UTF_8.name())

        return "/gateway/${entityRef.getAppName()}/api/ecos/webapp/content?ref=$refEnc&att=$attEnc"
    }

    fun getRefFromContentUrl(url: String?): EntityRef {
        if (url.isNullOrBlank()) {
            return EntityRef.EMPTY
        }
        val refArg = "?ref="
        val refArgIdx = url.indexOf(refArg)
        if (!url.startsWith(CONTENT_URL_PATH_GATEWAY_PART) || refArgIdx == -1) {
            return EntityRef.EMPTY
        }
        var refParamEnd = url.indexOf('&', refArgIdx)
        if (refParamEnd == -1) {
            refParamEnd = url.length
        }
        val encodedRef = url.substring(refArgIdx + refArg.length, refParamEnd)
        val entityRef = URLDecoder.decode(encodedRef, Charsets.UTF_8.name()).toEntityRef()

        if (entityRef.getAppName().isEmpty()) {
            val slashIdx = url.indexOf('/', CONTENT_URL_PATH_GATEWAY_PART.length)
            if (slashIdx != -1) {
                return entityRef.withAppName(url.substring(CONTENT_URL_PATH_GATEWAY_PART.length, slashIdx))
            }
        }
        return entityRef
    }

    //todo: compare with getRefFromContentUrl and replace with single method or rename any function
    private fun getRecordRefFromContentUrl(url: String): EntityRef {
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

    fun uploadContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storage: EcosContentStorageConfig?,
        creatorRefId: Long,
        writer: (EcosContentWriter) -> Unit
    ): Long? {
        val contentService = ctx.contentService ?: return null
        return contentService.uploadContent(name, mimeType, encoding, storage, creatorRefId, writer).getDbId()
    }

    fun uploadContent(
        record: DbEntity,
        attribute: String,
        contentData: DataValue,
        multiple: Boolean,
        storage: EcosContentStorageConfig?,
        creatorRefId: Long
    ): Any? {

        val contentService = ctx.contentService ?: return null

        if (contentData.isArray()) {
            if (contentData.size() == 0) {
                return null
            }
            return if (multiple) {
                val newArray = DataValue.createArr()
                contentData.forEach {
                    val contentId = uploadContent(record, attribute, it, false, storage, creatorRefId)
                    if (contentId != null) {
                        newArray.add(contentId)
                    }
                }
                newArray
            } else {
                uploadContent(record, attribute, contentData[0], false, storage, creatorRefId)
            }
        }
        if (!contentData.isObject()) {
            if (contentData.isLong()) {
                if (isContentDbDataAware() || AuthContext.isRunAsSystem()) {
                    return contentService.cloneContent(contentData.asLong(), creatorRefId).getDbId()
                }
            } else if (contentData.isTextual()) {
                val contentText = contentData.asText()
                val appNameDelimIdx = contentText.indexOf(EntityRef.APP_NAME_DELIMITER)
                val srcIdDelimIdx = contentText.indexOf(EntityRef.SOURCE_ID_DELIMITER)
                if (srcIdDelimIdx != -1 && appNameDelimIdx < srcIdDelimIdx) {
                    return uploadFromEntity(EntityRef.valueOf(contentText), storage, creatorRefId)
                }
            }
            return null
        }
        val urlData = contentData["url"]
        var dataBytes: ByteArray? = null
        var mimeType = ""
        var name = ""

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
                mimeType = data.mimeType
                name = contentData["originalName"].asText()
            }
        }
        if (dataBytes == null && contentData.has("data")) {
            val data = contentData["data"]
            if (!data.isObject()) {
                return null
            }
            name = contentData["originalName"].asText().ifBlank { contentData["name"].asText() }
            val entityRef = data["entityRef"].asText()
            if (entityRef.isNotBlank()) {
                return uploadFromEntity(EntityRef.valueOf(entityRef), storage, creatorRefId)
            } else {
                val nodeRef = data["nodeRef"].asText()
                if (nodeRef.isNotBlank()) {
                    val ref = EntityRef.create(AppName.ALFRESCO, "", nodeRef)
                    val content = ctx.recordsService.getAtts(ref, AlfContentAtts::class.java)
                    dataBytes = content.bytes
                    mimeType = content.mimetype ?: "application/octet-stream"
                    name = name.ifBlank { content.name ?: "" }
                } else {
                    return null
                }
            }
        }
        if (dataBytes == null) {
            return null
        }
        return contentService.uploadContent(name, mimeType, null, storage, creatorRefId) {
            it.writeBytes(dataBytes)
        }.getDbId()
    }

    private fun uploadFromEntity(entityRef: EntityRef, storage: EcosContentStorageConfig?, creatorRefId: Long): Long? {

        val contentService = ctx.contentService ?: return null

        if (entityRef.getAppName() == ctx.appName) {
            val atts = withContentDbDataAware {
                ctx.recordsService.getAtts(entityRef, ContentLocationAtts::class.java)
            }
            if (isContentMayBeCloned(atts, storage)) {
                return contentService.cloneContent(atts.contentId!!, creatorRefId).getDbId()
            }
        }
        ctx.contentApi ?: error("Content API is null")
        val metaAtts = ctx.recordsService.getAtts(entityRef, ContentMetaAtts::class.java)
        return if (metaAtts.mimeType.isBlank()) {
            null
        } else {
            ctx.contentApi.getContent(entityRef)?.readContent { input ->
                contentService.uploadContent(
                    name = metaAtts.name,
                    mimeType = metaAtts.mimeType,
                    encoding = metaAtts.encoding,
                    storage = storage,
                    creatorRefId = creatorRefId
                ) { it.writeStream(input) }.getDbId()
            }
        }
    }

    private fun isContentMayBeCloned(
        contentAtts: ContentLocationAtts,
        newContentStorage: EcosContentStorageConfig?
    ): Boolean {
        if (contentAtts.dbSchema != ctx.tableRef.schema ||
            contentAtts.contentId == null || contentAtts.contentId < 0
        ) {
            return false
        }
        var storageRef = newContentStorage?.ref ?: EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        if (storageRef == EcosContentStorageConstants.DEFAULT_CONTENT_STORAGE_REF) {
            storageRef = EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        }
        return contentAtts.storageRef == storageRef
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
        val contentId: Long?,
        @AttName("_content.storageRef?id")
        val storageRef: EntityRef?
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
