package ru.citeck.ecos.data.sql.domain

import ru.citeck.ecos.data.sql.content.DbContentService
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterImpl
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.computed.DbComputedAttsComponent
import ru.citeck.ecos.data.sql.records.listener.DbRecordsListener
import ru.citeck.ecos.data.sql.records.perms.DbPermsComponent
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefService
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.OutputStream

class DbDomainFactory(
    val ecosTypeRepo: TypesRepo,
    val dataSource: DbDataSource?,
    val dataServiceFactory: DbDataServiceFactory,
    val permsComponent: DbPermsComponent,
    val computedAttsComponent: DbComputedAttsComponent,
    val recordRefServiceSupplier: (DbDataSource, String) -> DbRecordRefService,
    val dbContentServiceSupplier: (DbDataSource, EcosContentWriterFactory, String) -> DbContentService,
    val defaultListeners: List<DbRecordsListener>
) {

    val contentWriterFactory: EcosContentWriterFactory = object : EcosContentWriterFactory {
        override fun createWriter(output: OutputStream): EcosContentWriter {
            return EcosContentWriterImpl(output)
        }
    }

    fun create(domainConfig: DbDomainConfig): Builder {
        return Builder(domainConfig)
    }

    inner class Builder(val domainConfig: DbDomainConfig) {

        var dataSource: DbDataSource? = null
        var permsComponent: DbPermsComponent? = null
        var computedAttsComponent: DbComputedAttsComponent? = null
        var dbContentService: DbContentService? = null
        var listeners: List<DbRecordsListener>? = null
        var excludeDefaultListeners: Boolean = false
        var contentWriterFactory: EcosContentWriterFactory? = null

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

        fun withEcosContentService(dbContentService: DbContentService): Builder {
            this.dbContentService = dbContentService
            return this
        }

        fun withListeners(listeners: List<DbRecordsListener>?): Builder {
            this.listeners = listeners
            return this
        }

        fun withExcludeDefaultListeners(excludeDefaultListeners: Boolean?): Builder {
            this.excludeDefaultListeners = excludeDefaultListeners ?: false
            return this
        }

        fun build(): DbRecordsDao {

            val dataSource = dataSource ?: this@DbDomainFactory.dataSource
                ?: error("dataSource is null")

            val dataService = DbDataServiceImpl(
                DbEntity::class.java,
                domainConfig.dataService,
                dataSource,
                dataServiceFactory
            )

            val schema = domainConfig.dataService.tableRef.schema

            val recordsDao = DbRecordsDao(
                domainConfig.recordsDao,
                ecosTypeRepo,
                dataService,
                recordRefServiceSupplier.invoke(dataSource, schema),
                permsComponent ?: this@DbDomainFactory.permsComponent,
                computedAttsComponent ?: this@DbDomainFactory.computedAttsComponent,
                this.dbContentService ?: dbContentServiceSupplier.invoke(
                    dataSource,
                    contentWriterFactory ?: this@DbDomainFactory.contentWriterFactory,
                    schema
                )
            )

            if (!excludeDefaultListeners) {
                this@DbDomainFactory.defaultListeners.forEach {
                    recordsDao.addListener(it)
                }
            }
            listeners?.forEach {
                recordsDao.addListener(it)
            }
            return recordsDao
        }
    }
}
