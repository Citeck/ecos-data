package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.schema.ScalarType
import ru.citeck.ecos.records3.record.atts.schema.SchemaAtt
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttContext
import ru.citeck.ecos.records3.record.atts.schema.resolver.AttSchemaUtils
import ru.citeck.ecos.records3.record.atts.value.AttValue
import ru.citeck.ecos.records3.record.atts.value.AttValueProxy
import ru.citeck.ecos.records3.record.atts.value.impl.AttValueDelegate
import ru.citeck.ecos.records3.record.atts.value.impl.InnerAttValue
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.promise.Promise

class DbDefaultRemoteContentValue(
    private val contentPath: String,
    private val innerPath: String,
    private val recordRef: EntityRef,
    private val ctx: DbRecordsDaoCtx,
    private val baseRecId: String,
    private val baseTypeId: String,
    private val name: String
) : AttValue, AttValueProxy {

    companion object {
        private val ATTS_WITHOUT_LOADING = setOf("_as", "name", "url")

        private const val AS_CONTENT_DATA_PATH = "_as.content-data"
    }

    private var size: Long = 0
    private var extension: String = ""
    private var innerAtts: AttValue? = null

    override fun init(): Promise<*>? {

        val innerAttsMap: MutableMap<String, String> = LinkedHashMap()
        val schemaWriter = ctx.schemaWriter

        var innerPathParts = emptyList<String>()
        val ctxAtts = if (innerPath == AS_CONTENT_DATA_PATH) {
            ArrayList()
        } else {
            val atts = AttContext.getCurrentSchemaAtt().inner.filterTo(ArrayList()) {
                !it.name.startsWith('$') && !ATTS_WITHOUT_LOADING.contains(it.name)
            }
            val wrappedAtts = wrapAtts(innerPath, atts)
            innerPathParts = wrappedAtts.first
            ArrayList(wrappedAtts.second)
        }

        ctxAtts.add(
            SchemaAtt.create()
                .withName("size")
                .withInner(SchemaAtt.create().withName("?num"))
                .build()
        )
        ctxAtts.add(
            SchemaAtt.create()
                .withName("extension")
                .withInner(SchemaAtt.create().withName("?str"))
                .build()
        )

        val (contentPathParts, schemaAttsToLoad) = wrapAtts(contentPath, AttSchemaUtils.simplifySchema(ctxAtts))
        for (att in schemaAttsToLoad) {
            innerAttsMap[att.getAliasForValue()] = schemaWriter.write(att)
        }
        var loadedInnerAtts = ctx.recordsService.getAtts(
            listOf(recordRef),
            innerAttsMap,
            true
        ).first().getAtts().getData()

        for (scopePart in contentPathParts) {
            loadedInnerAtts = loadedInnerAtts[scopePart]
        }
        this.size = loadedInnerAtts["size"]["?num"].asLong()
        this.extension = loadedInnerAtts["extension"]["?str"].asText()

        innerAtts = if (innerPath == AS_CONTENT_DATA_PATH) {
            ctx.attValuesConverter.toAttValue(getContentData())
        } else {
            if (innerPath.isEmpty()) {
                if (loadedInnerAtts.has("?json")) {
                    loadedInnerAtts["?json"] = loadedInnerAtts["?json"].copy()
                        .set("name", getNameWithExt())
                        .set("url", getUrl())
                }
                InnerAttsWrapper(InnerAttValue(loadedInnerAtts))
            } else {
                for (part in innerPathParts) {
                    loadedInnerAtts = loadedInnerAtts[part]
                }
                InnerAttValue(loadedInnerAtts)
            }
        }
        return null
    }

    private fun wrapAtts(path: String, attributes: List<SchemaAtt>): Pair<List<String>, List<SchemaAtt>> {
        if (path.isEmpty()) {
            return emptyList<String>() to attributes
        }
        val parts = path.split(".")
        var attribute = SchemaAtt.create()
            .withName(parts.last())
            .withInner(attributes)
        for (idx in (parts.lastIndex - 1) downTo 0) {
            attribute = SchemaAtt.create()
                .withName(parts[idx])
                .withInner(attribute)
        }
        return parts to listOf(attribute.build())
    }

    override fun getDisplayName(): Any {
        return getNameWithExt()
    }

    override fun getAtt(name: String): Any? {
        return innerAtts?.getAtt(name)
    }

    override fun asJson(): Any? {
        return innerAtts?.asJson()
    }

    private fun getUrl(): String {
        return ctx.recContentHandler.createContentUrl(baseRecId, RecordConstants.ATT_CONTENT)
    }

    private fun getNameWithExt(): String {
        if (extension.isNotEmpty()) {
            val extensionWithDot = ".$extension"
            if (!name.endsWith(extensionWithDot)) {
                return name + extensionWithDot
            }
        }
        return name
    }

    private fun getContentData(): DbContentValue.ContentData {
        return DbContentValue.ContentData(
            url = getUrl(),
            name = getNameWithExt(),
            recordRef = ctx.getGlobalRef(baseRecId),
            fileType = baseTypeId,
            size = size
        )
    }

    private inner class InnerAttsWrapper(value: AttValue) : AttValueDelegate(value), AttValueProxy {

        override fun getAtt(name: String): Any? {
            return when (name) {
                RecordConstants.ATT_AS -> AsValue()
                ScalarType.DISP.mirrorAtt, DbContentValue.ATT_NAME -> getNameWithExt()
                ScalarType.JSON.mirrorAtt -> asJson()
                DbContentValue.ATT_URL -> getUrl()
                else -> impl.getAtt(name)
            }
        }
    }

    private inner class AsValue : AttValue {

        override fun getAtt(name: String): Any? {
            if (name == "content-data") {
                return getContentData()
            }
            return null
        }
    }
}
