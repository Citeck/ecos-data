package ru.citeck.ecos.data.sql.remote.action

import ru.citeck.ecos.commons.json.serialization.annotation.IncludeNonDefault
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.util.IdentityHashMap

class UpdateRemoteAssocsAction : DbRemoteActionExecutor<UpdateRemoteAssocsAction.Params> {

    companion object {
        const val TYPE = "update-remote-assocs"
    }

    private lateinit var webAppApi: EcosWebAppApi
    private lateinit var recordsService: RecordsService

    private var currentAppName = ""

    override fun init(webAppApi: EcosWebAppApi, recordsService: RecordsService) {
        this.webAppApi = webAppApi
        this.recordsService = recordsService
        currentAppName = webAppApi.getProperties().appName
    }

    override fun execute(action: Params): Response {

        val creatorRef = webAppApi.getAuthoritiesApi().getAuthorityRef(action.creator)

        val schemaCtxBySrcId: MutableMap<String, DbSchemaContext?> = IdentityHashMap()
        fillSchemaCtxBySrcId(action.add, AssocsDiff::refs, schemaCtxBySrcId)
        fillSchemaCtxBySrcId(action.rem, AssocsDiff::refs, schemaCtxBySrcId)

        var addedCount = 0

        action.add.forEach { toAdd ->
            val refsBySchema = groupBySchema(toAdd.refs, schemaCtxBySrcId)
            refsBySchema.forEach { (schemaCtx, refs) ->

                val allRefs = HashSet<EntityRef>(refs)
                allRefs.add(action.srcRef)
                allRefs.add(creatorRef)
                val refIds = schemaCtx.recordRefService.getOrCreateIdByEntityRefsMap(allRefs)

                val creatorRefId = refIds[creatorRef] ?: error("Creator id is not found")
                val srcRefId = refIds[action.srcRef] ?: error("Src ref id is not found")
                val targetRefsIds = refs.map {
                    refIds[it] ?: error("Ref id is not found for $it")
                }
                val createdAssocIds = schemaCtx.assocsService.createAssocs(
                    srcRefId,
                    toAdd.assocId,
                    false,
                    targetRefsIds,
                    creatorRefId
                )
                addedCount += createdAssocIds.size
            }
        }

        var removedCount = 0

        action.rem.forEach { toRem ->

            val refsBySchema = groupBySchema(toRem.refs, schemaCtxBySrcId)
            refsBySchema.forEach { (schemaCtx, refs) ->

                val allRefs = HashSet<EntityRef>(refs)
                allRefs.add(action.srcRef)
                val refIds = schemaCtx.recordRefService.getOrCreateIdByEntityRefsMap(allRefs)

                val srcRefId = refIds[action.srcRef] ?: error("Src ref id is not found")
                val targetRefsIds = refs.map {
                    refIds[it] ?: error("Ref id is not found for $it")
                }
                val removedAssocsIds = schemaCtx.assocsService.removeAssocs(
                    srcRefId,
                    toRem.assocId,
                    targetRefsIds,
                    true
                )
                removedCount += removedAssocsIds.size
            }
        }

        return Response(addedCount, removedCount)
    }

    private fun groupBySchema(
        refs: List<EntityRef>,
        schemaCtxBySrcId: Map<String, DbSchemaContext?>
    ): Map<DbSchemaContext, List<EntityRef>> {
        val result = HashMap<DbSchemaContext, MutableList<EntityRef>>()
        for (ref in refs) {
            val ctx = schemaCtxBySrcId[ref.getSourceId()] ?: continue
            result.computeIfAbsent(ctx) { ArrayList() }.add(ref)
        }
        return result
    }

    private fun <T> fillSchemaCtxBySrcId(
        elements: Collection<T>,
        getRefs: (T) -> List<EntityRef>,
        schemaCtxBySrcId: MutableMap<String, DbSchemaContext?>
    ) {
        for (element in elements) {
            for (ref in getRefs(element)) {
                if (ref.getAppName().isNotEmpty() && ref.getAppName() != currentAppName) {
                    continue
                }
                schemaCtxBySrcId.computeIfAbsent(ref.getSourceId()) {
                    val dao = recordsService.getRecordsDao(it) ?: error("Records DAO doesn't found by id $it")
                    if (dao is DbRecordsDao) {
                        dao.getRecordsDaoCtx().dataService.getTableContext().getSchemaCtx()
                    } else {
                        null
                    }
                }
            }
        }
    }

    override fun getType(): String {
        return TYPE
    }

    class Response(
        val added: Int,
        val removed: Int
    )

    @IncludeNonDefault
    class Params(
        val srcRef: EntityRef = EntityRef.EMPTY,
        val creator: String = "",
        val add: List<AssocsDiff> = emptyList(),
        val rem: List<AssocsDiff> = emptyList()
    )

    class AssocsDiff(
        val assocId: String,
        val refs: List<EntityRef>,
    )
}
