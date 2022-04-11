package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.content.EcosContentMeta
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.request.RequestContext

class DbContentValue(
    private val ctx: DbRecordsDaoCtx,
    private val recId: String,
    private val name: MLText,
    private val contentId: Long,
    private val attribute: String
) : AttValue {

    companion object {
        const val CONTENT_DATA = "content-data"
    }

    private val meta: EcosContentMeta by lazy {
        val service = ctx.contentService ?: error("Content service is null")
        service.getMeta(contentId) ?: error("Content doesn't found by id '$id'")
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            "name" -> meta.name
            "sha256" -> meta.sha256
            "size" -> meta.size
            "mimeType" -> meta.mimeType
            "encoding" -> meta.encoding
            "created" -> meta.created
            else -> null
        }
    }

    override fun getAs(type: String): Any? {
        if (type == CONTENT_DATA) {
            val name = if (MLText.isEmpty(name)) {
                meta.name
            } else {
                MLText.getClosestValue(name, RequestContext.getLocale())
            }
            return ContentData(
                ctx.recContentHandler.createContentUrl(recId, attribute),
                name,
                meta.size
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
