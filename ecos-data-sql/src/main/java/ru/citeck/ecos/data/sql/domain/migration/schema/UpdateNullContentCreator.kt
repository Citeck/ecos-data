package ru.citeck.ecos.data.sql.domain.migration.schema

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthUser
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class UpdateNullContentCreator : DbSchemaMigration {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    override fun run(context: DbSchemaContext) {

        val contentServiceImpl = context.contentService as? DbContentServiceImpl ?: return
        val dataService = contentServiceImpl.getDataService()

        log.info { "Migration started" }

        dataService.runMigrations(mock = false, diff = true)
        dataService.resetColumnsCache()

        val anonCreatorId = context.recordRefService.getOrCreateIdByEntityRef(
            EntityRef.create(AppName.EMODEL, "person", AuthUser.ANONYMOUS)
        )

        val tableRef = context.getTableRef(DbContentEntity.TABLE)
        val updateSql = "UPDATE ${tableRef.fullName} SET " +
            "${DbContentEntity.CREATOR}=? WHERE ${DbContentEntity.CREATOR} " +
            "IS NULL OR ${DbContentEntity.CREATOR} = -1;"

        val updateRes = context.dataSourceCtx.dataSource.update(updateSql, listOf(anonCreatorId))

        log.info { "Migration completed. Updated: $updateRes" }
    }

    override fun getAppliedVersions(): Int {
        return 5
    }
}
