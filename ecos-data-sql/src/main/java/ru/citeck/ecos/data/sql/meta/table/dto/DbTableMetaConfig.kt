package ru.citeck.ecos.data.sql.meta.table.dto

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig

@JsonDeserialize(builder = DbTableMetaConfig.Builder::class)
data class DbTableMetaConfig(
    val dataServiceConfig: DbDataServiceConfig
) {
    companion object {

        @JvmField
        val DEFAULT = DbTableMetaConfig(DbDataServiceConfig.EMPTY)

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
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
