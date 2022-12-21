package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbEcosContentData
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records3.record.atts.value.AttValue

class DbContentValue(
    private val ctx: DbRecordsDaoCtx,
    private val recId: String,
    private val contentDbId: Long,
    private val attribute: String
) : AttValue, HasEcosContentDbData {

    companion object {
        const val CONTENT_DATA = "content-data"

        const val ATT_NAME = "name"
        const val ATT_SHA256 = "sha256"
        const val ATT_SIZE = "size"
        const val ATT_MIME_TYPE = "mimeType"
        const val ATT_ENCODING = "encoding"
        const val ATT_CREATED = "created"
        const val ATT_BYTES = "bytes"
    }

    val contentData: DbEcosContentData by lazy {
        val service = ctx.contentService ?: error("Content service is null")
        service.getContent(contentDbId) ?: error("Content doesn't found by id '$id'")
    }

    override fun getContentDbData(): DbEcosContentData {
        return contentData
    }

    override fun getDisplayName(): Any {
        return contentData.getName()
    }

    override fun asText(): String {
        return contentData.getName()
    }

    override fun asJson(): Any {
        val data = mutableMapOf<String, Any>()
        data[ATT_NAME] = contentData.getName()
        data[ATT_SHA256] = contentData.getSha256()
        data[ATT_SIZE] = contentData.getSize()
        data[ATT_MIME_TYPE] = contentData.getMimeType()
        data[ATT_ENCODING] = contentData.getEncoding()
        data[ATT_CREATED] = contentData.getCreated()
        return data
    }
    override fun asDouble(): Double {
        return contentData.getSize().toDouble()
    }

    override fun asBoolean(): Boolean {
        return true
    }

    override fun asRaw(): Any {
        return asJson()
    }

    override fun asBin(): Any {
        return contentData.readContent { it.readBytes() }
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            ATT_NAME -> contentData.getName()
            ATT_SHA256 -> contentData.getSha256()
            ATT_SIZE -> contentData.getSize()
            ATT_MIME_TYPE -> contentData.getMimeType()
            ATT_ENCODING -> contentData.getEncoding()
            ATT_CREATED -> contentData.getCreated()
            ATT_BYTES -> contentData.readContent { it.readBytes() }
            "uri" -> {
                return if (AuthContext.isRunAsSystem() || AuthContext.isRunAsAdmin()) {
                    contentData.getUri()
                } else {
                    null
                }
            }
            "tableRef", "contentDbId" -> {
                if (!ctx.recContentHandler.isContentDbDataAware()) {
                    return null
                }
                when (name) {
                    "tableRef" -> ctx.tableRef
                    "contentDbId" -> contentDbId
                    else -> null
                }
            }
            "previewInfo" -> {
                val mimeType = contentData.getMimeType()
                if (mimeType.startsWith("image/") || mimeType == "application/pdf" || mimeType == "plain/text") {
                    val url = ctx.recContentHandler.createContentUrl(recId, attribute)
                    val extension = contentData.getName().substringBeforeLast(".", "")
                    return DataValue.createObj()
                        .set("url", url)
                        .set("originalUrl", url)
                        .set("originalName", contentData.getName())
                        .set("originalExt", extension)
                        .set("ext", extension)
                        .set("mimetype", contentData.getMimeType())
                } else {
                    return null
                }
            }
            else -> null
        }
    }

    override fun getAs(type: String): Any? {
        if (type == CONTENT_DATA) {
            return ContentData(
                ctx.recContentHandler.createContentUrl(recId, attribute),
                contentData.getName(),
                contentData.getSize()
            )
        }
        return null
    }

    data class ContentData(
        val url: String,
        val name: String,
        val size: Long
    )
}
