package ru.citeck.ecos.data.sql.pg.records.commons

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.procstages.dto.ProcStageDef
import ru.citeck.ecos.model.lib.status.dto.StatusDef
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.model.lib.type.dto.TypeAspectDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class TypeRegistration(
    private var typeId: String,
    private var sourceId: String,
    private val register: (TypeInfo) -> DbRecordsTestBase.RecordsDaoTestCtx
) {

    private var extIdTemplate: String? = null
    private var parentRef: EntityRef = EntityRef.EMPTY
    private var numTemplateRef: EntityRef = EntityRef.EMPTY
    private var queryPermsPolicy: QueryPermsPolicy = QueryPermsPolicy.DEFAULT
    private var workspaceScope: WorkspaceScope = WorkspaceScope.PUBLIC
    private var defaultWorkspace: String = ""
    private var defaultStatus: String = ""
    private var attributes: List<AttributeDef> = emptyList()
    private var sysAttributes: List<AttributeDef> = emptyList()
    private var statuses: List<StatusDef> = emptyList()
    private var stages: List<ProcStageDef> = emptyList()
    private val aspects: MutableList<TypeAspectDef> = ArrayList()

    fun withId(id: String): TypeRegistration {
        this.typeId = id
        return this
    }

    fun withExtIdTemplate(extIdTemplate: String): TypeRegistration {
        this.extIdTemplate = extIdTemplate
        return this
    }

    fun withNumTemplateRef(numTemplateRef: EntityRef): TypeRegistration {
        this.numTemplateRef = numTemplateRef
        return this
    }

    fun withQueryPermsPolicy(queryPermsPolicy: QueryPermsPolicy): TypeRegistration {
        this.queryPermsPolicy = queryPermsPolicy
        return this
    }

    fun withParentType(parentTypeId: String): TypeRegistration {
        this.parentRef = ModelUtils.getTypeRef(parentTypeId)
        return this
    }

    fun asSubTypeWithId(typeId: String): TypeRegistration {
        withParentType(this.typeId)
        return withId(typeId)
    }

    fun withDefaultStatus(defaultStatus: String): TypeRegistration {
        this.defaultStatus = defaultStatus
        return this
    }

    fun withWorkspaceScope(workspaceScope: WorkspaceScope): TypeRegistration {
        this.workspaceScope = workspaceScope
        return this
    }

    fun withDefaultWorkspace(defaultWorkspace: String): TypeRegistration {
        this.defaultWorkspace = defaultWorkspace
        return this
    }

    fun withAttributes(vararg attributes: AttributeDef.Builder): TypeRegistration {
        this.attributes = attributes.map { it.build() }
        return this
    }

    fun withStatuses(statuses: List<StatusDef.Builder>): TypeRegistration {
        this.statuses = statuses.map { it.build() }
        return this
    }

    fun withStatuses(vararg statuses: StatusDef.Builder): TypeRegistration {
        this.statuses = statuses.map { it.build() }
        return this
    }

    fun withStages(stages: List<ProcStageDef.Builder>): TypeRegistration {
        this.stages = stages.map { it.build() }
        return this
    }

    fun withStages(vararg stages: ProcStageDef.Builder): TypeRegistration {
        this.stages = stages.map { it.build() }
        return this
    }

    fun addAspect(aspect: TypeAspectDef.Builder): TypeRegistration {
        aspects.add(aspect.build())
        return this
    }

    fun addAspect(aspectId: String): TypeRegistration {
        aspects.add(TypeAspectDef.create().withRef(ModelUtils.getAspectRef(aspectId)).build())
        return this
    }

    fun withAttributes(vararg attributes: AttributeDef): TypeRegistration {
        this.attributes = attributes.toList()
        return this
    }

    fun withSysAttributes(vararg attributes: AttributeDef): TypeRegistration {
        this.sysAttributes = attributes.toList()
        return this
    }

    fun withSourceId(sourceId: String): TypeRegistration {
        this.sourceId = sourceId
        return this
    }

    fun register(): DbRecordsTestBase.RecordsDaoTestCtx {
        return register(
            TypeInfo.create()
                .withId(typeId)
                .withExtIdTemplate(extIdTemplate)
                .withQueryPermsPolicy(queryPermsPolicy)
                .withNumTemplateRef(numTemplateRef)
                .withParentRef(parentRef)
                .withSourceId(sourceId)
                .withWorkspaceScope(workspaceScope)
                .withDefaultWorkspace(defaultWorkspace)
                .withDefaultStatus(defaultStatus)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(attributes)
                        .withSystemAttributes(sysAttributes)
                        .withStatuses(statuses)
                        .withStages(stages)
                        .build()
                )
                .withAspects(aspects)
                .build()
        )
    }
}
