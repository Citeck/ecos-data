package ru.citeck.ecos.data.sql.pg.spring

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.content.EcosContentService
import ru.citeck.ecos.data.sql.content.EcosContentServiceImpl
import ru.citeck.ecos.data.sql.content.data.EcosContentDataServiceImpl
import ru.citeck.ecos.data.sql.content.data.storage.local.DbContentDataEntity
import ru.citeck.ecos.data.sql.content.data.storage.local.EcosContentLocalStorage
import ru.citeck.ecos.data.sql.content.entity.DbContentEntity
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.domain.DbDomainFactory
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.pg.PgDataServiceFactory
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

@Configuration
open class EcosDataRecordsDaoFactoryConfig {

    private val recordRefServiceBySchema: ConcurrentHashMap<String, DbRecordRefService> = ConcurrentHashMap()
    private val contentServiceBySchema: ConcurrentHashMap<String, EcosContentService> = ConcurrentHashMap()

    @Autowired
    private lateinit var dbDataSource: DbDataSource
    @Autowired
    private lateinit var dbDataServiceFactory: DbDataServiceFactory

    private fun getRecordRefService(schema: String): DbRecordRefService {

        return recordRefServiceBySchema.computeIfAbsent(schema) {
            DbRecordRefService(
                DbDataServiceImpl(
                    DbRecordRefEntity::class.java,
                    DbDataServiceConfig.create()
                        .withTableRef(DbTableRef(schema, "ecos_record_ref"))
                        .build(),
                    dbDataSource,
                    dbDataServiceFactory
                )
            )
        }
    }

    private fun getContentServiceBySchema(schema: String): EcosContentService {

        return contentServiceBySchema.computeIfAbsent(schema) {

            val contentDataService = EcosContentDataServiceImpl()
            contentDataService.register(
                EcosContentLocalStorage(
                    DbDataServiceImpl(
                        DbContentDataEntity::class.java,
                        DbDataServiceConfig.create()
                            .withTableRef(DbTableRef(schema, "ecos_content_data"))
                            .withStoreTableMeta(true)
                            .build(),
                        dbDataSource,
                        dbDataServiceFactory
                    )
                )
            )
            EcosContentServiceImpl(
                DbDataServiceImpl(
                    DbContentEntity::class.java,
                    DbDataServiceConfig.create()
                        .withTableRef(DbTableRef(schema, "ecos_content"))
                        .withStoreTableMeta(true)
                        .build(),
                    dbDataSource,
                    dbDataServiceFactory
                ),
                contentDataService
            )
        }
    }

    @Bean
    open fun dbDataServiceFactory(): DbDataServiceFactory {
        return PgDataServiceFactory()
    }

    @Bean
    open fun dbDataSource(dataSource: DataSource): DbDataSource {
        return DbDataSourceImpl(dataSource)
    }

    @Bean
    open fun dbDomainFactory(
        ecosTypesRepo: TypesRepo,
        recordsService: RecordsService,
        permsComponent: DbPermsComponent,
        modelServiceFactory: ModelServiceFactory
    ): DbDomainFactory {

        val computedAttsComponent = object : DbComputedAttsComponent {
            override fun computeAttsToStore(value: Any, isNewRecord: Boolean, typeRef: RecordRef): ObjectData {
                return modelServiceFactory.computedAttsService.computeAttsToStore(value, isNewRecord, typeRef)
            }
            override fun computeDisplayName(value: Any, typeRef: RecordRef): MLText {
                return modelServiceFactory.computedAttsService.computeDisplayName(value, typeRef)
            }
        }

        return DbDomainFactory(
            ecosTypesRepo,
            dbDataSource,
            dbDataServiceFactory,
            permsComponent,
            computedAttsComponent,
            { schema -> getRecordRefService(schema) },
            { schema -> getContentServiceBySchema(schema) }
        )
    }
}
