package ru.citeck.ecos.data.sql.records.dao.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.utils.DataUriUtil
import ru.citeck.ecos.data.sql.content.EcosContentMeta
import ru.citeck.ecos.data.sql.content.data.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.net.URLDecoder
import java.net.URLEncoder

class DbRecContentHandler(private val ctx: DbRecordsDaoCtx) {

    fun createContentUrl(recordId: String, attribute: String): String {

        val recordsDaoIdEnc = URLEncoder.encode(ctx.sourceId, Charsets.UTF_8.name())
        val recordIdEnc = URLEncoder.encode(recordId, Charsets.UTF_8.name())
        val attributeEnc = URLEncoder.encode(attribute, Charsets.UTF_8.name())

        return "/gateway/${ctx.appName}/api/record-content/$recordsDaoIdEnc/$recordIdEnc/$attributeEnc"
    }

    fun getRecordRefFromContentUrl(url: String): RecordRef {
        if (url.isBlank()) {
            return RecordRef.EMPTY
        }
        val parts = url.split("/")
        if (parts.size != 7) {
            error("Unexpected URL parts size: ${parts.size}. Url: " + url)
        }
        val appName = parts[2]
        val recordsDaoId = URLDecoder.decode(parts[5], Charsets.UTF_8.name())
        val recId = URLDecoder.decode(parts[6], Charsets.UTF_8.name())

        if (recId.isNotBlank()) {
            return RecordRef.create(appName, recordsDaoId, recId)
        }
        return RecordRef.EMPTY
    }

    fun getRefForContentData(value: DataValue): RecordRef {

        val url = value.get("url").asText()
        if (url.isBlank()) {
            return RecordRef.EMPTY
        }
        if (url.startsWith("/share/page/card-details")) {
            val nodeRef = value.get("data").get("nodeRef").asText()
            if (nodeRef.isNotBlank()) {
                return RecordRef.create("alfresco", "", nodeRef)
            }
        } else if (url.startsWith("/gateway")) {
            return getRecordRefFromContentUrl(url)
        }
        return RecordRef.EMPTY
    }

    fun uploadContent(
        record: DbEntity,
        attribute: String,
        contentData: DataValue,
        multiple: Boolean
    ): Any? {

        val contentService = ctx.contentService ?: return null

        if (contentData.isArray()) {
            if (contentData.size() == 0) {
                return null
            }
            return if (multiple) {
                val newArray = DataValue.createArr()
                contentData.forEach {
                    val contentId = uploadContent(record, attribute, it, false)
                    if (contentId != null) {
                        newArray.add(contentId)
                    }
                }
                newArray
            } else {
                uploadContent(record, attribute, contentData[0], false)
            }
        }
        if (!contentData.isObject()) {
            return null
        }
        val urlData = contentData["url"]
        val dataBytes: ByteArray?
        val contentMeta: EcosContentMeta

        if (urlData.isTextual() && urlData.isNotEmpty()) {

            val recordContentUrl = createContentUrl(record.extId, attribute)

            if (urlData.asText() == recordContentUrl) {
                return record.attributes[attribute]
            }

            val data = DataUriUtil.parseData(urlData.asText())
            dataBytes = data.data
            if (dataBytes == null || dataBytes.isEmpty()) {
                return null
            }
            contentMeta = EcosContentMeta.create {
                withMimeType(data.mimeType)
                withName(contentData["originalName"].asText())
            }
        } else if (contentData.has("data")) {
            val data = contentData["data"]
            if (!data.isObject()) {
                return null
            }
            val name = contentData["originalName"].asText().ifBlank { contentData["name"].asText() }
            val nodeRef = data["nodeRef"].asText()
            if (nodeRef.isNotBlank()) {
                val ref = EntityRef.create("alfresco", "", nodeRef)
                val content = ctx.recordsService.getAtts(ref, AlfContentAtts::class.java)
                dataBytes = content.bytes
                contentMeta = EcosContentMeta.create {
                    withMimeType(content.mimetype ?: "application/octet-stream")
                    withName(name.ifBlank { content.name ?: "" })
                }
            } else {
                return null
            }
        } else {
            return null
        }
        if (dataBytes == null) {
            return null
        }
        return contentService.writeContent(EcosContentLocalStorage.TYPE, contentMeta, dataBytes).id
    }

    private class AlfContentAtts(
        @AttName("?disp")
        val name: String?,
        @AttName("_content.bytes")
        val bytes: ByteArray?,
        @AttName("_content.mimetype")
        val mimetype: String?
    )
}
