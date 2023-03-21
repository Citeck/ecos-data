package ru.citeck.ecos.data.sql.context

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.DbContentServiceImpl
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageService
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageServiceImpl
import ru.citeck.ecos.data.sql.content.storage.local.DbContentDataEntity
import ru.citeck.ecos.data.sql.content.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterImpl
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaService
import ru.citeck.ecos.data.sql.meta.schema.DbSchemaMetaServiceImpl
import ru.citeck.ecos.data.sql.meta.table.DbTableMetaEntity
import ru.citeck.ecos.data.sql.perms.DbEntityPermsService
import ru.citeck.ecos.data.sql.perms.DbEntityPermsServiceImpl
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.OutputStream

class DbSchemaContext(
    val schema: String,
    val dataSourceCtx: DbDataSourceContext,
) {
    val schemaMetaService: DbSchemaMetaService = DbSchemaMetaServiceImpl(this)

    val tableMetaService: DbDataService<DbTableMetaEntity> = DbDataServiceImpl(
        DbTableMetaEntity::class.java,
        DbDataServiceConfig.create()
            .withTable(DbTableMetaEntity.TABLE)
            .build(),
        this
    )
    val authorityDataService: DbDataService<DbAuthorityEntity> = DbDataServiceImpl(
        DbAuthorityEntity::class.java,
        DbDataServiceConfig.create()
            .withTable(DbAuthorityEntity.TABLE)
            .build(),
        this
    )
    val entityPermsService: DbEntityPermsService = DbEntityPermsServiceImpl(this)

    val recordRefService: DbRecordRefService = DbRecordRefService(dataSourceCtx.appName, this)

    private val contentWriterFactory: EcosContentWriterFactory = object : EcosContentWriterFactory {
        override fun createWriter(output: OutputStream): EcosContentWriter {
            return EcosContentWriterImpl(output)
        }
    }

    val contentStorageService: EcosContentStorageService = EcosContentStorageServiceImpl(contentWriterFactory)
    val contentService: DbContentService = DbContentServiceImpl(contentStorageService, this)

    init {
        contentStorageService.register(
            EcosContentLocalStorage(
                DbDataServiceImpl(
                    DbContentDataEntity::class.java,
                    DbDataServiceConfig.create {
                        withTable(DbContentDataEntity.TABLE)
                        withStoreTableMeta(true)
                    },
                    this
                )
            )
        )
    }
}
