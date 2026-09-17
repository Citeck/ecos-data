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

    /**
     * The columns **this** data service may read from or write to.
     *
     * Backup columns left behind by a type migration are excluded, because they hold the only copy
     * of the user's pre-migration data and nothing outside the migration machinery may address one.
     * The exception is a service built with `includeBackupColumns`, which the migration builds for
     * itself and nobody else does - for such a service this returns the backups too, which is why
     * it is not simply "the columns that are not backups".
     *
     * Backups are recognised **by name**
     * ([ru.citeck.ecos.data.sql.columnmeta.DbBackupColumnNames.isBackupName]) and by nothing else -
     * no registry row is read to build this list. The registry uses the `__is_backup` flag with the
     * prefix as its backstop; the two describe the same set, because an attribute id cannot begin
     * with `_`. Going by the name costs no query and keeps a `__backup_...` column invisible here
     * whatever `ed_column_meta` says, which is what makes an installation upgraded from before the
     * registry existed safe.
     */
    fun getColumns(): List<DbColumnDef>

    /**
     * Every physical column of the table, backups included, whatever this service is configured to
     * address. Only the migration machinery has any business with it - see [getColumns].
     */
    fun getAllPhysicalColumns(): List<DbColumnDef>

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
