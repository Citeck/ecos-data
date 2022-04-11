package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint

class DbDataServiceConfig(
    val tableRef: DbTableRef,
    val maxItemsToAllowSchemaMigration: Long,
    val fkConstraints: List<DbFkConstraint>,
    val authEnabled: Boolean,
    val storeTableMeta: Boolean,
    val transactional: Boolean
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbDataServiceConfig {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var tableRef: DbTableRef = DbTableRef("", "")
        var maxItemsToAllowSchemaMigration: Long = 1000

        var authEnabled: Boolean = false
        var storeTableMeta: Boolean = false
        var transactional: Boolean = false
        var fkConstraints: List<DbFkConstraint> = emptyList()

        constructor(base: DbDataServiceConfig) : this() {
            tableRef = base.tableRef
            maxItemsToAllowSchemaMigration = base.maxItemsToAllowSchemaMigration
            authEnabled = base.authEnabled
            storeTableMeta = base.storeTableMeta
            transactional = base.transactional
            fkConstraints = base.fkConstraints
        }

        fun withAuthEnabled(authEnabled: Boolean?): Builder {
            this.authEnabled = authEnabled ?: EMPTY.authEnabled
            return this
        }

        fun withMaxItemsToAllowSchemaMigration(maxItemsToAllowSchemaMigration: Long?): Builder {
            this.maxItemsToAllowSchemaMigration = maxItemsToAllowSchemaMigration ?: EMPTY.maxItemsToAllowSchemaMigration
            return this
        }

        fun withStoreTableMeta(storeTableMeta: Boolean?): Builder {
            this.storeTableMeta = storeTableMeta ?: EMPTY.storeTableMeta
            return this
        }

        fun withTransactional(transactional: Boolean?): Builder {
            this.transactional = transactional ?: EMPTY.transactional
            return this
        }

        fun withTableRef(tableRef: DbTableRef): Builder {
            this.tableRef = tableRef
            return this
        }

        fun withFkConstraints(fkConstraints: List<DbFkConstraint>?): Builder {
            this.fkConstraints = fkConstraints ?: emptyList()
            return this
        }

        fun build(): DbDataServiceConfig {
            return DbDataServiceConfig(
                tableRef,
                maxItemsToAllowSchemaMigration,
                fkConstraints,
                authEnabled,
                storeTableMeta,
                transactional
            )
        }
    }
}
