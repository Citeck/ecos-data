package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbTableContextImpl(
    private val table: String,
    private val columns: List<DbColumnDef> = emptyList(),
    val schemaCtx: DbSchemaContext,
    private val defaultPermsPolicy: TypePermsPolicy = TypePermsPolicy.PUBLIC
) : DbTableContext {

    private val tableRef = DbTableRef(schemaCtx.schema, table)
    private val columnsByName = columns.associateBy { it.name }
    private val hasIdColumn = columnsByName.containsKey(DbEntity.ID)
    private val hasDeleteFlag = columnsByName.containsKey(DbEntity.DELETED)

    override fun getRecordRefsService(): DbRecordRefService {
        return schemaCtx.recordRefService
    }

    override fun getContentService(): DbContentService {
        return schemaCtx.contentService
    }

    override fun getPermsService(): DbEntityPermsService {
        return schemaCtx.entityPermsService
    }

    override fun getTableRef(): DbTableRef {
        return tableRef
    }

    override fun getColumns(): List<DbColumnDef> {
        return columns
    }

    override fun getColumnByName(name: String): DbColumnDef? {
        return columnsByName[name]
    }

    override fun hasColumn(name: String): Boolean {
        return columnsByName.containsKey(name)
    }

    override fun hasIdColumn(): Boolean {
        return hasIdColumn
    }

    override fun hasDeleteFlag(): Boolean {
        return hasDeleteFlag
    }

    override fun getDataSource(): DbDataSource {
        return schemaCtx.dataSourceCtx.dataSource
    }

    override fun getTypesConverter(): DbTypesConverter {
        return schemaCtx.dataSourceCtx.converter
    }

    override fun getDefaultPermsPolicy(): TypePermsPolicy {
        return defaultPermsPolicy
    }

    override fun getCurrentUserAuthorityIds(): Set<Long> {
        val authorityEntities = DbDataReqContext.doWithPermsPolicy(TypePermsPolicy.PUBLIC) {
            schemaCtx.authorityDataService.findAll(
                Predicates.`in`(DbAuthorityEntity.EXT_ID, DbRecordsUtils.getCurrentAuthorities())
            )
        }

        return authorityEntities.map { it.id }.toSet()
    }

    fun withColumns(columns: List<DbColumnDef>): DbTableContextImpl {
        return DbTableContextImpl(
            table,
            columns,
            schemaCtx,
            defaultPermsPolicy
        )
    }
}
