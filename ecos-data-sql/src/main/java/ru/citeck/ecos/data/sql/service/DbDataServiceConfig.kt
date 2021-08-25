package ru.citeck.ecos.data.sql.service

class DbDataServiceConfig(
    val authEnabled: Boolean,
    val maxItemsToAllowSchemaMigration: Long
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        fun create(): Builder {
            return Builder()
        }

        fun create(builder: Builder.() -> Unit): DbDataServiceConfig {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var authEnabled: Boolean = false
        var maxItemsToAllowSchemaMigration: Long = 10

        constructor(base: DbDataServiceConfig) : this() {
            authEnabled = base.authEnabled
            maxItemsToAllowSchemaMigration = base.maxItemsToAllowSchemaMigration
        }

        fun withAuthEnabled(authEnabled: Boolean?): Builder {
            this.authEnabled = authEnabled ?: EMPTY.authEnabled
            return this
        }

        fun withMaxItemsToAllowSchemaMigration(maxItemsToAllowSchemaMigration: Long?): Builder {
            this.maxItemsToAllowSchemaMigration = maxItemsToAllowSchemaMigration ?: EMPTY.maxItemsToAllowSchemaMigration
            return this
        }

        fun build(): DbDataServiceConfig {
            return DbDataServiceConfig(
                authEnabled,
                maxItemsToAllowSchemaMigration
            )
        }
    }
}
