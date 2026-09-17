package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy

class DbDataServiceConfig(
    val table: String,
    /**
     * No longer consulted. Column migrations are governed by
     * `ecos.webapp.data.columns.inPlaceAlterMaxRows` and by the background migration, which have no
     * reason to refuse a change outright. Kept so that callers configuring it keep compiling.
     */
    @Deprecated("Not used anymore. See DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows")
    val maxItemsToAllowSchemaMigration: Long,
    val fkConstraints: List<DbFkConstraint>,
    val storeTableMeta: Boolean,
    val defaultQueryPermsPolicy: QueryPermsPolicy,
    /**
     * Whether backup columns left behind by a type migration are visible to this service.
     *
     * `false` everywhere but the column-migration handler, which is the one component whose job is
     * to read them. They are kept out of every other path precisely because a backup holds the
     * only copy of the user's pre-migration data.
     */
    val includeBackupColumns: Boolean = false
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
        var includeBackupColumns: Boolean = false

        @Suppress("DEPRECATION")
        constructor(base: DbDataServiceConfig) : this() {
            table = base.table
            maxItemsToAllowSchemaMigration = base.maxItemsToAllowSchemaMigration
            storeTableMeta = base.storeTableMeta
            fkConstraints = base.fkConstraints
            defaultQueryPermsPolicy = base.defaultQueryPermsPolicy
            includeBackupColumns = base.includeBackupColumns
        }

        @Deprecated("Not used anymore. See DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows")
        @Suppress("DEPRECATION")
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

        fun withIncludeBackupColumns(includeBackupColumns: Boolean?): Builder {
            this.includeBackupColumns = includeBackupColumns ?: EMPTY.includeBackupColumns
            return this
        }

        fun build(): DbDataServiceConfig {
            return DbDataServiceConfig(
                table,
                maxItemsToAllowSchemaMigration,
                fkConstraints,
                storeTableMeta,
                defaultQueryPermsPolicy,
                includeBackupColumns
            )
        }
    }
}
