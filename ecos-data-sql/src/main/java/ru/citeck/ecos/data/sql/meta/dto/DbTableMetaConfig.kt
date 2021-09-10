package ru.citeck.ecos.data.sql.meta.dto

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig

@JsonDeserialize(builder = DbTableMetaConfig.Builder::class)
data class DbTableMetaConfig(
    val dataServiceConfig: DbDataServiceConfig
) {
    companion object {

        @JvmField
        val DEFAULT = DbTableMetaConfig(DbDataServiceConfig.EMPTY)

        fun create(): Builder {
            return Builder()
        }

        fun create(builder: Builder.() -> Unit): DbTableMetaConfig {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var dataServiceConfig: DbDataServiceConfig = DbDataServiceConfig.EMPTY

        constructor(base: DbTableMetaConfig) : this() {
            dataServiceConfig = base.dataServiceConfig
        }

        fun withDataServiceConfig(dataServiceConfig: DbDataServiceConfig): Builder {
            this.dataServiceConfig = dataServiceConfig
            return this
        }

        fun build(): DbTableMetaConfig {
            return DbTableMetaConfig(dataServiceConfig)
        }
    }
}
