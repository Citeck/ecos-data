package ru.citeck.ecos.data.sql.domain

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize
import ru.citeck.ecos.data.sql.content.DbContentConfig
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig

@JsonDeserialize(builder = DbDomainConfig.Builder::class)
class DbDomainConfig(
    val dataService: DbDataServiceConfig,
    val recordsDao: DbRecordsDaoConfig,
    val content: DbContentConfig
) {

    companion object {

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbDomainConfig {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder {

        var dataService: DbDataServiceConfig? = null
        var recordsDao: DbRecordsDaoConfig? = null
        var content: DbContentConfig = DbContentConfig.DEFAULT

        fun withDataService(dataService: DbDataServiceConfig): Builder {
            this.dataService = dataService
            return this
        }

        fun withRecordsDao(recordsDao: DbRecordsDaoConfig): Builder {
            this.recordsDao = recordsDao
            return this
        }

        fun withContent(content: DbContentConfig): Builder {
            this.content = content
            return this
        }

        fun build(): DbDomainConfig {

            return DbDomainConfig(
                dataService ?: error("DataService config is a mandatory"),
                recordsDao ?: error("RecordsDao config is a mandatory"),
                content
            )
        }
    }
}
