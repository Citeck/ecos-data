package ru.citeck.ecos.data.sql.domain

import ru.citeck.ecos.data.sql.content.EcosContentService
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.type.repo.TypesRepo

class DbDomainFactory(
    val ecosTypeRepo: TypesRepo,
    val dataSource: DbDataSource,
    val dataServiceFactory: DbDataServiceFactory,
    val permsComponent: DbPermsComponent,
    val computedAttsComponent: DbComputedAttsComponent,
    val recordRefServiceSupplier: (String) -> DbRecordRefService,
    val ecosContentServiceSupplier: (String) -> EcosContentService
) {

    fun create(domainConfig: DbDomainConfig): Builder {
        return Builder(domainConfig)
    }

    inner class Builder(val domainConfig: DbDomainConfig) {

        var dataSource: DbDataSource? = null
        var permsComponent: DbPermsComponent? = null
        var computedAttsComponent: DbComputedAttsComponent? = null
        var ecosContentService: EcosContentService? = null

        fun withDataSource(dataSource: DbDataSource): Builder {
            this.dataSource = dataSource
            return this
        }

        fun withPermsComponent(permsComponent: DbPermsComponent): Builder {
            this.permsComponent = permsComponent
            return this
        }

        fun withComputedAttsComponent(computedAttsComponent: DbComputedAttsComponent): Builder {
            this.computedAttsComponent = computedAttsComponent
            return this
        }

        fun withEcosContentService(ecosContentService: EcosContentService): Builder {
            this.ecosContentService = ecosContentService
            return this
        }

        fun build(): DbRecordsDao {

            val dataSource = dataSource ?: this@DbDomainFactory.dataSource

            val dataService = DbDataServiceImpl(
                DbEntity::class.java,
                domainConfig.dataService,
                dataSource,
                dataServiceFactory
            )

            val schema = domainConfig.dataService.tableRef.schema

            return DbRecordsDao(
                domainConfig.recordsDao,
                ecosTypeRepo,
                dataService,
                recordRefServiceSupplier.invoke(schema),
                permsComponent ?: this@DbDomainFactory.permsComponent,
                computedAttsComponent ?: this@DbDomainFactory.computedAttsComponent,
                this.ecosContentService ?: ecosContentServiceSupplier.invoke(schema)
            )
        }
    }
}
