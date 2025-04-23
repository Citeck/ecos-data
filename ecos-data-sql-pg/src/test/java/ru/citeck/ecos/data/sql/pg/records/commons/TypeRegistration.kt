package ru.citeck.ecos.data.sql.pg.records.commons

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypeAspectDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.model.lib.utils.ModelUtils

class TypeRegistration(
    private var typeId: String,
    private var sourceId: String,
    private val register: (TypeInfo) -> DbRecordsTestBase.RecordsDaoTestCtx
) {

    private var workspaceScope: WorkspaceScope = WorkspaceScope.PUBLIC
    private var defaultWorkspace: String = ""
    private var attributes: List<AttributeDef> = emptyList()
    private val aspects: MutableList<TypeAspectDef> = ArrayList()

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

    fun withSourceId(sourceId: String): TypeRegistration {
        this.sourceId = sourceId
        return this
    }

    fun register(): DbRecordsTestBase.RecordsDaoTestCtx {
        return register(
            TypeInfo.create()
                .withId(typeId)
                .withSourceId(sourceId)
                .withWorkspaceScope(workspaceScope)
                .withDefaultWorkspace(defaultWorkspace)
                .withModel(
                    TypeModelDef.create()
                        .withAttributes(attributes)
                        .build()
                )
                .withAspects(aspects)
                .build()
        )
    }
}
