package ru.citeck.ecos.data.sql.pg

import org.postgresql.jdbc.PgArray
import org.postgresql.util.PGobject
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.datasource.DbDataSourceImpl
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.DbTableMetaEntity
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.data.sql.type.DbTypesConverter
import javax.sql.DataSource

class PgDataServiceFactory {

    companion object {
        private const val META_TABLE_NAME = "ecos_data_table_meta"
    }

    fun <T : Any> create(entityType: Class<T>): Builder<T> {
        return Builder(entityType)
    }

    inner class Builder<T : Any>(private val entityType: Class<T>) {

        private lateinit var config: DbDataServiceConfig
        private lateinit var tableRef: DbTableRef
        private lateinit var dataSource: DbDataSource
        private lateinit var dbContextManager: DbContextManager
        private var storeTableMeta: Boolean = true
        private var transactional: Boolean = true

        lateinit var schemaDao: DbSchemaDaoPg

        fun withConfig(config: DbDataServiceConfig): Builder<T> {
            this.config = config
            return this
        }

        fun withTableRef(tableRef: DbTableRef): Builder<T> {
            this.tableRef = tableRef
            return this
        }

        fun withDataSource(dbDataSource: DbDataSource): Builder<T> {
            this.dataSource = dbDataSource
            return this
        }

        fun withDataSource(dataSource: DataSource): Builder<T> {
            this.dataSource = DbDataSourceImpl(dataSource)
            return this
        }

        fun withDbContextManager(dbContextManager: DbContextManager): Builder<T> {
            this.dbContextManager = dbContextManager
            return this
        }

        fun withStoreTableMeta(storeTableMeta: Boolean): Builder<T> {
            this.storeTableMeta = storeTableMeta
            return this
        }

        fun withTransactional(transactional: Boolean): Builder<T> {
            this.transactional = transactional
            return this
        }

        fun build(): DbDataService<T> {

            val tableMetaService = if (storeTableMeta) {
                create(DbTableMetaEntity::class.java)
                    .withTableRef(DbTableRef(tableRef.schema, META_TABLE_NAME))
                    .withDataSource(dataSource)
                    .withConfig(DbDataServiceConfig(false))
                    .withDbContextManager(dbContextManager)
                    .withStoreTableMeta(false)
                    .withTransactional(false)
                    .build()
            } else {
                null
            }

            val txnDataService = if (transactional) {
                create(entityType)
                    .withTableRef(DbTableRef(tableRef.schema, tableRef.table + "__ext_txn"))
                    .withDataSource(dataSource)
                    .withConfig(DbDataServiceConfig(false))
                    .withDbContextManager(dbContextManager)
                    .withStoreTableMeta(false)
                    .withTransactional(false)
                    .build()
            } else {
                null
            }

            val typesConverter = DbTypesConverter()
            typesConverter.register(PgArray::class) { it.array }
            typesConverter.register(PGobject::class) { it.value }

            val entityMapper = DbEntityMapperImpl(entityType.kotlin, typesConverter)
            schemaDao = DbSchemaDaoPg(dataSource, tableRef)
            val entityRepo = DbEntityRepoPg(entityMapper, dbContextManager, dataSource, tableRef, typesConverter)

            return DbDataServiceImpl(
                config,
                tableRef,
                dataSource,
                entityType.kotlin,
                schemaDao,
                entityRepo,
                tableMetaService,
                txnDataService
            )
        }
    }
}
