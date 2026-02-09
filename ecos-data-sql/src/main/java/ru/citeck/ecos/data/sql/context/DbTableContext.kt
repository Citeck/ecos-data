package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.assocs.DbAssocsService
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.records.workspace.DbWorkspaceService
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.webapp.api.authority.EcosAuthoritiesApi
import kotlin.reflect.KClass

interface DbTableContext {

    fun getWorkspaceService(): DbWorkspaceService

    fun getPermsService(): DbEntityPermsService

    fun getAssocsService(): DbAssocsService

    fun getRecordRefsService(): DbRecordRefService

    fun getContentService(): DbContentService

    fun getAuthoritiesApi(): EcosAuthoritiesApi

    fun getColumns(): List<DbColumnDef>

    fun getTableRef(): DbTableRef

    fun getEntityValueTypeForColumn(name: String?): KClass<*>

    fun getColumnByName(name: String?): DbColumnDef?

    fun hasColumn(name: String?): Boolean

    fun hasIdColumn(): Boolean

    fun getDataSource(): DbDataSource

    fun getTypesConverter(): DbTypesConverter

    fun getAuthoritiesIdsMap(authorities: Collection<String>): Map<String, Long>

    fun getQueryPermsPolicy(): QueryPermsPolicy

    fun isSameSchema(other: DbTableContext): Boolean

    fun getSchemaCtx(): DbSchemaContext
}
