package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbEcosContentData
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.mime.MimeType

class DbContentValue(
    private val ctx: DbRecordsDaoCtx,
    private val recId: String,
    private val contentDbId: Long,
    private val attribute: String,
    private val isDefaultContent: Boolean
) : AttValue, HasEcosContentDbData {

    companion object {
        const val CONTENT_DATA = "content-data"

        const val ATT_NAME = "name"
        const val ATT_EXTENSION = "ext"
        const val ATT_SHA256 = "sha256"
        const val ATT_URL = "url"
        const val ATT_SIZE = "size"
        const val ATT_MIME_TYPE = "mimeType"
        const val ATT_ENCODING = "encoding"
        const val ATT_CREATED = "created"
        const val ATT_CREATOR = "creator"
        const val ATT_BYTES = "bytes"

        const val ATT_PREVIEW_INFO = "previewInfo"
        const val ATT_CONVERTED_TO = "convertedTo"

        private const val PREVIEW_INFO_ATT_URL = "url"
        private const val PREVIEW_INFO_ATT_EXT = "ext"
        private const val PREVIEW_INFO_ATT_MIME_TYPE = "mimetype"
        private const val PREVIEW_INFO_ATT_ORIGINAL_URL = "originalUrl"
        private const val PREVIEW_INFO_ATT_ORIGINAL_NAME = "originalName"
        private const val PREVIEW_INFO_ATT_ORIGINAL_EXT = "originalExt"

        private const val TRANSFORM_WEBAPI_PATH = "/tfm/transform"
    }

    private val currentEntityRef = EntityRef.create(ctx.appName, ctx.sourceId, recId)

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
        val data = mutableMapOf<String, Any?>()
        data[ATT_NAME] = contentData.getName()
        data[ATT_EXTENSION] = getAtt(ATT_EXTENSION) as? String ?: ""
        data[ATT_SHA256] = contentData.getSha256()
        data[ATT_SIZE] = contentData.getSize()
        data[ATT_MIME_TYPE] = contentData.getMimeType()
        data[ATT_ENCODING] = contentData.getEncoding()
        data[ATT_CREATED] = contentData.getCreated()
        data[ATT_CREATOR] = contentData.getCreator()
        data[ATT_URL] = getAtt(ATT_URL) as? String ?: ""
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
            ATT_EXTENSION -> contentData.getName().substringAfterLast('.')
            ATT_SHA256 -> contentData.getSha256()
            ATT_SIZE -> contentData.getSize()
            ATT_MIME_TYPE -> contentData.getMimeType()
            ATT_ENCODING -> contentData.getEncoding()
            ATT_CREATED -> contentData.getCreated()
            ATT_CREATOR -> contentData.getCreator()
            ATT_URL -> ctx.recContentHandler.createContentUrl(currentEntityRef, attribute)
            ATT_BYTES -> contentData.readContent { it.readBytes() }
            ATT_CONVERTED_TO -> ConvertedToValue()
            "uri" -> {
                return if (AuthContext.isRunAsSystem() || AuthContext.isRunAsAdmin()) {
                    contentData.getUri()
                } else {
                    null
                }
            }
            "tableRef", "contentDbId" -> {
                if (!AuthContext.isRunAsSystem() && !ctx.recContentHandler.isContentDbDataAware()) {
                    return null
                }
                when (name) {
                    "tableRef" -> ctx.tableRef
                    "contentDbId" -> contentDbId
                    else -> null
                }
            }
            ATT_PREVIEW_INFO -> {

                val origUrl = ctx.recContentHandler.createContentUrl(currentEntityRef, attribute)
                val origMimeType = contentData.getMimeType()
                val origExtension = contentData.getName().substringAfterLast(".", "")

                val previewData = if (
                    origMimeType.getType() == "image" ||
                    origMimeType == MimeTypes.APP_PDF
                ) {
                    PreviewData(origUrl, origExtension, origMimeType)
                } else if (isDefaultContent) {

                    val attsToLoad = listOf(
                        ATT_URL,
                        ATT_EXTENSION,
                        ATT_MIME_TYPE
                    )

                    val atts = ctx.recordsService.queryOne(
                        RecordsQuery.create {
                            withEcosType("thumbnail")
                            withQuery(
                                Predicates.and(
                                    Predicates.eq(RecordConstants.ATT_PARENT, currentEntityRef),
                                    Predicates.eq("mimeType", MimeTypes.APP_PDF_TEXT),
                                    Predicates.eq("srcAttribute", RecordConstants.ATT_CONTENT)
                                )
                            )
                        },
                        attsToLoad.associateWith {
                            "${RecordConstants.ATT_CONTENT}.$it"
                        }
                    )
                    if (atts == null || attsToLoad.any { atts[it].asText().isEmpty() }) {
                        null
                    } else {
                        PreviewData(
                            atts[ATT_URL].asText(),
                            atts[ATT_EXTENSION].asText(),
                            MimeTypes.parse(atts[ATT_MIME_TYPE].asText())
                        )
                    }
                } else {
                    null
                } ?: return null

                return DataValue.createObj()
                    .set(PREVIEW_INFO_ATT_URL, previewData.url)
                    .set(PREVIEW_INFO_ATT_EXT, previewData.ext)
                    .set(PREVIEW_INFO_ATT_MIME_TYPE, previewData.mimeType)
                    .set(PREVIEW_INFO_ATT_ORIGINAL_URL, origUrl)
                    .set(PREVIEW_INFO_ATT_ORIGINAL_NAME, contentData.getName())
                    .set(PREVIEW_INFO_ATT_ORIGINAL_EXT, origExtension)
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

    private inner class ConvertedToValue : AttValue {

        override fun getAtt(name: String): Any? {
            return when (name) {
                "text" -> {
                    val mimeType = contentData.getMimeType()
                    if (mimeType == MimeTypes.TXT_PLAIN || mimeType == MimeTypes.APP_BIN) {
                        contentData.readContent { String(it.readBytes()) }
                    } else {
                        val client = ctx.webApiClient ?: error("Web API client is null")
                        client.newRequest()
                            .targetApp(AppName.TRANSFORMATIONS)
                            .path(TRANSFORM_WEBAPI_PATH)
                            .header(
                                "contentMeta",
                                DataValue.createObj()
                                    .set("mimeType", mimeType.toString())
                                    .set("name", contentData.getName())
                                    .set("sha256", contentData.getSha256())
                                    .set("size", contentData.getSize())
                            )
                            .header(
                                "transformations",
                                DataValue.createArr()
                                    .add(
                                        DataValue.createObj()
                                            .set("type", "convert")
                                            .set("config", DataValue.createObj().set("toMimeType", MimeTypes.TXT_PLAIN))
                                    )
                            )
                            .body { bodyWriter ->
                                contentData.readContent {
                                    it.copyTo(bodyWriter.getOutputStream())
                                }
                            }
                            .executeSync { it.getBodyReader().readAsText() }
                    }
                }
                else -> null
            }
        }
    }

    data class ContentData(
        val url: String,
        val name: String,
        val size: Long
    )

    private data class PreviewData(
        val url: String,
        val ext: String,
        val mimeType: MimeType
    )
}
