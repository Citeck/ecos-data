package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.context.lib.i18n.I18nContext
import ru.citeck.ecos.data.sql.content.EcosContentDbData
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records3.record.atts.value.AttValue

class DbContentValue(
    private val ctx: DbRecordsDaoCtx,
    private val recId: String,
    private val name: MLText,
    private val contentDbId: Long,
    private val attribute: String
) : AttValue {

    companion object {
        const val CONTENT_DATA = "content-data"
    }

    val contentData: EcosContentDbData by lazy {
        val service = ctx.contentService ?: error("Content service is null")
        service.getContent(contentDbId) ?: error("Content doesn't found by id '$id'")
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            "name" -> contentData.getName()
            "sha256" -> contentData.getSha256()
            "size" -> contentData.getSize()
            "mimeType" -> contentData.getMimeType()
            "encoding" -> contentData.getEncoding()
            "created" -> contentData.getCreated()
            "bytes" -> contentData.readContent { it.readBytes() }
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
            else -> null
        }
    }

    override fun getAs(type: String): Any? {
        if (type == CONTENT_DATA) {
            val name = if (MLText.isEmpty(name)) {
                contentData.getName()
            } else {
                MLText.getClosestValue(name, I18nContext.getLocale())
            }
            return ContentData(
                ctx.recContentHandler.createContentUrl(recId, attribute),
                name,
                contentData.getSize(),
                contentData.getName()
            )
        }
        return null
    }

    data class ContentData(
        val url: String,
        val name: String,
        val size: Long,
        val contentName: String
    )
}
