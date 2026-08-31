package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.DbEcosContentData
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.dao.query.dto.query.RecordsQuery
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.mime.ContentPreviewKind
import ru.citeck.ecos.webapp.api.mime.MimeType

class DbContentValue(
    private val ctx: DbRecordsDaoCtx,
    private val recId: String,
    private val recType: TypeInfo?,
    private val contentDbId: Long,
    private val attribute: String,
    private val isDefaultContent: Boolean
) : AttValue, HasEcosContentDbData {

    companion object {
        const val CONTENT_DATA = "content-data"

        const val ATT_NAME = "name"
        const val ATT_CONTENT_NAME = "contentName"
        const val ATT_EXTENSION = "ext"
        const val ATT_SHA256 = "sha256"
        const val ATT_URL = "url"
        const val ATT_SIZE = "size"
        const val ATT_MIME_TYPE = "mimeType"
        const val ATT_ENCODING = "encoding"
        const val ATT_CREATED = "created"
        const val ATT_CREATOR = "creator"
        const val ATT_BYTES = "bytes"

        // internal
        const val ATT_STORAGE_REF = "storageRef"
        const val ATT_CONTENT_DBID = "contentDbId"
        const val ATT_TABLE_REF = "tableRef"
        // --------

        const val ATT_PREVIEW_INFO = "previewInfo"
        const val ATT_CONVERTED_TO = "convertedTo"

        const val PREVIEW_INFO_ATT_ORIGINAL_NAME = "originalName"

        private const val PREVIEW_INFO_ATT_URL = "url"
        private const val PREVIEW_INFO_ATT_EXT = "ext"

        // legacy lowercase key, kept alongside the camelCase one
        private const val PREVIEW_INFO_ATT_MIME_TYPE_LEGACY = "mimetype"
        private const val PREVIEW_INFO_ATT_MIME_TYPE = "mimeType"
        private const val PREVIEW_INFO_ATT_SIZE = "size"
        private const val PREVIEW_INFO_ATT_KIND = "kind"
        private const val PREVIEW_INFO_ATT_STATUS = "status"
        private const val PREVIEW_INFO_ATT_ORIGINAL_URL = "originalUrl"
        private const val PREVIEW_INFO_ATT_ORIGINAL_EXT = "originalExt"
        private const val PREVIEW_INFO_ATT_ORIGINAL_MIME_TYPE = "originalMimeType"
        private const val PREVIEW_INFO_ATT_ORIGINAL_SIZE = "originalSize"

        private const val THUMBNAIL_TYPE_ID = "thumbnail"
        private const val THUMBNAIL_ATT_STATUS = "status"
        private const val THUMBNAIL_ATT_SRC_ATTRIBUTE = "srcAttribute"

        private const val THUMBNAIL_STATUS_PROCESSED = "PROCESSED"
        private const val THUMBNAIL_STATUS_FAILED = "FAILED"

        private const val TRANSFORM_WEBAPI_PATH = "/tfm/transform"

        /**
         * Wire form of the [ContentPreviewKind] and [PreviewStatus] enums. Both are rendered the
         * same way so that neither of them needs a second list of literals next to its constants.
         */
        private fun toPreviewInfoValue(value: Enum<*>): String {
            return value.name.lowercase()
        }
    }

    private val currentEntityRef: EntityRef = ctx.getGlobalRef(recId)

    // For the default content attribute, build content URLs against the system `_content`
    // attribute so the download endpoint substitutes the entity display name into
    // Content-Disposition. Named (non-default) content attributes keep their own attribute in the URL.
    private val urlAttribute: String
        get() = if (isDefaultContent) {
            RecordConstants.ATT_CONTENT
        } else {
            attribute
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

    override fun asJson(): MutableMap<String, Any?> {
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
            ATT_CONTENT_NAME -> contentData.getName()
            ATT_EXTENSION -> contentData.getName().substringAfterLast('.')
            ATT_SHA256 -> contentData.getSha256()
            ATT_SIZE -> contentData.getSize()
            ATT_MIME_TYPE -> contentData.getMimeType()
            ATT_ENCODING -> contentData.getEncoding()
            ATT_CREATED -> contentData.getCreated()
            ATT_CREATOR -> contentData.getCreator()
            ATT_URL -> ctx.recContentHandler.createContentUrl(currentEntityRef, urlAttribute)
            ATT_BYTES -> contentData.readContent { it.readBytes() }
            ATT_CONVERTED_TO -> ConvertedToValue()
            "dataKey" -> {
                if (AuthContext.isRunAsSystem() || AuthContext.isRunAsAdmin()) {
                    contentData.getDataKey()
                } else {
                    null
                }
            }
            ATT_TABLE_REF, ATT_CONTENT_DBID, ATT_STORAGE_REF -> {
                if (!AuthContext.isRunAsSystem() && !ctx.recContentHandler.isContentDbDataAware()) {
                    return null
                }
                when (name) {
                    ATT_TABLE_REF -> ctx.tableRef
                    ATT_CONTENT_DBID -> contentDbId
                    ATT_STORAGE_REF -> contentData.getStorageRef()
                    else -> null
                }
            }
            ATT_PREVIEW_INFO -> getPreviewInfo()
            else -> null
        }
    }

    /**
     * Describe what the UI may render for this content and in which state that renderable thing is.
     *
     * The result is never null for an existing content: when there is nothing to render the
     * renderable part is empty ([ContentPreviewKind.NONE] and a blank url) and the status says why,
     * while the original always stays fully described so that at least a download link can be built.
     */
    private fun getPreviewInfo(): DataValue {

        val origUrl = ctx.recContentHandler.createContentUrl(currentEntityRef, urlAttribute)
        val origMimeType = contentData.getMimeType()
        val origName = contentData.getName()
        val origExtension = origName.substringAfterLast(".", "")
        val origSize = contentData.getSize()

        val preview = resolvePreview(origUrl, origMimeType, origExtension, origSize)

        return DataValue.createObj()
            .set(PREVIEW_INFO_ATT_URL, preview.url)
            .set(PREVIEW_INFO_ATT_EXT, preview.ext)
            .set(PREVIEW_INFO_ATT_MIME_TYPE_LEGACY, preview.mimeType)
            .set(PREVIEW_INFO_ATT_MIME_TYPE, preview.mimeType)
            .set(PREVIEW_INFO_ATT_SIZE, preview.size)
            .set(PREVIEW_INFO_ATT_KIND, toPreviewInfoValue(preview.kind))
            .set(PREVIEW_INFO_ATT_STATUS, toPreviewInfoValue(preview.status))
            .set(PREVIEW_INFO_ATT_ORIGINAL_URL, origUrl)
            .set(PREVIEW_INFO_ATT_ORIGINAL_NAME, origName)
            .set(PREVIEW_INFO_ATT_ORIGINAL_EXT, origExtension)
            .set(PREVIEW_INFO_ATT_ORIGINAL_MIME_TYPE, origMimeType.toString())
            .set(PREVIEW_INFO_ATT_ORIGINAL_SIZE, origSize)
    }

    private fun resolvePreview(
        origUrl: String,
        origMimeType: MimeType,
        origExtension: String,
        origSize: Long
    ): PreviewData {

        val nativeKind = origMimeType.getPreviewKind()
        if (nativeKind != ContentPreviewKind.NONE) {
            return PreviewData(
                origUrl,
                origExtension,
                origMimeType.toString(),
                origSize,
                nativeKind,
                PreviewStatus.READY
            )
        }

        // findThumbnail is one query per content value, and previewInfo is requested over whole
        // lists of records, so widening it past the default content attribute would multiply that
        // N+1 by the number of content attributes a record has. Kept to the default attribute, as
        // it was before chunked upload, until the lookup is batched over a page of records.
        if (!isDefaultContent) {
            return PreviewData.nothing(PreviewStatus.UNSUPPORTED)
        }

        // A converter declared for this mime type gets its thumbnail record created in the same
        // transaction as the content change, so the absence of the record means no converter at all.
        val thumbnail = findThumbnail() ?: return PreviewData.nothing(PreviewStatus.UNSUPPORTED)

        return when (thumbnail[THUMBNAIL_ATT_STATUS].asText()) {
            THUMBNAIL_STATUS_PROCESSED -> thumbnailPreview(thumbnail)
            THUMBNAIL_STATUS_FAILED -> PreviewData.nothing(PreviewStatus.FAILED)
            else -> PreviewData.nothing(PreviewStatus.PROCESSING)
        }
    }

    private fun thumbnailPreview(thumbnail: RecordAtts): PreviewData {

        val url = thumbnail[ATT_URL].asText()
        if (url.isBlank()) {
            return PreviewData.nothing(PreviewStatus.FAILED)
        }
        val mimeType = thumbnail[ATT_MIME_TYPE].asText()
        val kind = MimeTypes.parseOrElse(mimeType, MimeTypes.APP_BIN).getPreviewKind()
        if (kind == ContentPreviewKind.NONE) {
            return PreviewData.nothing(PreviewStatus.FAILED)
        }
        return PreviewData(
            url,
            thumbnail[ATT_EXTENSION].asText(),
            mimeType,
            thumbnail[ATT_SIZE].asLong(0L),
            kind,
            PreviewStatus.READY
        )
    }

    /**
     * Thumbnails are bound to the source attribute they were generated from, so the lookup is keyed
     * by the attribute this value represents. Only [resolvePreview]'s default-content gate reaches
     * here, so that key is always `_content` today - which is also the only source automatic
     * thumbnail creation ever records. A record without content is a different outcome than a
     * missing record and is reported by the blank content attributes of the returned atts.
     */
    private fun findThumbnail(): RecordAtts? {

        val attsToLoad = mapOf(
            ATT_URL to "${RecordConstants.ATT_CONTENT}.$ATT_URL",
            ATT_EXTENSION to "${RecordConstants.ATT_CONTENT}.$ATT_EXTENSION",
            ATT_MIME_TYPE to "${RecordConstants.ATT_CONTENT}.$ATT_MIME_TYPE",
            ATT_SIZE to "${RecordConstants.ATT_CONTENT}.$ATT_SIZE?num",
            THUMBNAIL_ATT_STATUS to THUMBNAIL_ATT_STATUS
        )

        // todo: replace runAsSystem to assoc with thumbnails
        return AuthContext.runAsSystem {
            ctx.recordsService.queryOne(
                RecordsQuery.create {
                    withEcosType(THUMBNAIL_TYPE_ID)
                    withQuery(
                        Predicates.and(
                            Predicates.eq(RecordConstants.ATT_PARENT, currentEntityRef),
                            Predicates.eq(ATT_MIME_TYPE, MimeTypes.APP_PDF_TEXT),
                            Predicates.eq(THUMBNAIL_ATT_SRC_ATTRIBUTE, urlAttribute)
                        )
                    )
                },
                attsToLoad
            )
        }
    }

    override fun getAs(type: String): Any? {
        if (type == CONTENT_DATA) {
            return ContentData(
                ctx.recContentHandler.createContentUrl(recId, urlAttribute),
                contentData.getName(),
                contentData.getSize(),
                currentEntityRef,
                recType?.id ?: ""
            )
        }
        return null
    }

    override fun equals(other: Any?): Boolean {
        other ?: return false
        if (this === other) {
            return true
        }
        if (other::class != this::class) {
            return false
        }
        other as DbContentValue
        return contentDbId == other.contentDbId
    }

    override fun hashCode(): Int {
        return contentDbId.hashCode()
    }

    private inner class ConvertedToValue : AttValue {

        override fun getAtt(name: String): Any? {
            return when (name) {
                "text" -> {
                    val mimeType = contentData.getMimeType()
                    if (mimeType.isTextual() || mimeType == MimeTypes.APP_BIN) {
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
        val size: Long,
        val recordRef: EntityRef,
        val fileType: String
    )

    /**
     * The renderable part of the preview info: what is at [url], not what the original is.
     */
    private data class PreviewData(
        val url: String,
        val ext: String,
        val mimeType: String,
        val size: Long,
        val kind: ContentPreviewKind,
        val status: PreviewStatus
    ) {
        companion object {
            fun nothing(status: PreviewStatus): PreviewData {
                return PreviewData("", "", "", 0L, ContentPreviewKind.NONE, status)
            }
        }
    }

    private enum class PreviewStatus {
        READY,
        PROCESSING,
        FAILED,
        UNSUPPORTED
    }
}
