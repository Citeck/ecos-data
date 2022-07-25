package ru.citeck.ecos.data.sql.records.dao.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.utils.DataUriUtil
import ru.citeck.ecos.data.sql.content.EcosContentMeta
import ru.citeck.ecos.data.sql.content.data.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.records2.RecordRef
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
        if (!urlData.isTextual()) {
            return null
        }
        val recordContentUrl = createContentUrl(record.extId, attribute)

        if (urlData.asText() == recordContentUrl) {
            return record.attributes[attribute]
        }

        val data = DataUriUtil.parseData(urlData.asText())
        val dataBytes = data.data
        if (dataBytes == null || dataBytes.isEmpty()) {
            return null
        }

        val contentMeta = EcosContentMeta.create {
            withMimeType(data.mimeType)
            withName(contentData["originalName"].asText())
        }

        return contentService.writeContent(EcosContentLocalStorage.TYPE, contentMeta, dataBytes).id
    }
}
