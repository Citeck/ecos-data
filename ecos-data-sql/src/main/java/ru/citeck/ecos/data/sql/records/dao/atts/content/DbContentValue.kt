package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.data.sql.content.EcosContentDbData
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
    }

    val contentData: EcosContentDbData by lazy {
        val service = ctx.contentService ?: error("Content service is null")
        service.getContent(contentDbId) ?: error("Content doesn't found by id '$id'")
    }

    override fun getContentDbData(): EcosContentDbData {
        return contentData
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
