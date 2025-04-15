package ru.citeck.ecos.data.sql.pg.records.commons

import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope

class TypeRegistration(
    private var typeId: String,
    private var sourceId: String,
    private val register: (TypeInfo) -> DbRecordsTestBase.RecordsDaoTestCtx
) {

    private var workspaceScope: WorkspaceScope = WorkspaceScope.PUBLIC
    private var defaultWorkspace: String = ""
    private var attributes: List<AttributeDef> = emptyList()

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
                .build()
        )
    }
}
