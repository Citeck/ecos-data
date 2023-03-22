package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy

class DbDataServiceConfig(
    val table: String,
    val maxItemsToAllowSchemaMigration: Long,
    val fkConstraints: List<DbFkConstraint>,
    val storeTableMeta: Boolean,
    val defaultQueryPermsPolicy: QueryPermsPolicy
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

        var table: String = ""
        var maxItemsToAllowSchemaMigration: Long = 1000

        var storeTableMeta: Boolean = false
        var fkConstraints: List<DbFkConstraint> = emptyList()
        var defaultQueryPermsPolicy: QueryPermsPolicy = QueryPermsPolicy.PUBLIC

        constructor(base: DbDataServiceConfig) : this() {
            table = base.table
            maxItemsToAllowSchemaMigration = base.maxItemsToAllowSchemaMigration
            storeTableMeta = base.storeTableMeta
            fkConstraints = base.fkConstraints
            defaultQueryPermsPolicy = base.defaultQueryPermsPolicy
        }

        fun withMaxItemsToAllowSchemaMigration(maxItemsToAllowSchemaMigration: Long?): Builder {
            this.maxItemsToAllowSchemaMigration = maxItemsToAllowSchemaMigration ?: EMPTY.maxItemsToAllowSchemaMigration
            return this
        }

        fun withStoreTableMeta(storeTableMeta: Boolean?): Builder {
            this.storeTableMeta = storeTableMeta ?: EMPTY.storeTableMeta
            return this
        }

        fun withTable(table: String): Builder {
            this.table = table
            return this
        }

        fun withFkConstraints(fkConstraints: List<DbFkConstraint>?): Builder {
            this.fkConstraints = fkConstraints ?: emptyList()
            return this
        }

        fun build(): DbDataServiceConfig {
            return DbDataServiceConfig(
                table,
                maxItemsToAllowSchemaMigration,
                fkConstraints,
                storeTableMeta,
                defaultQueryPermsPolicy
            )
        }
    }
}
