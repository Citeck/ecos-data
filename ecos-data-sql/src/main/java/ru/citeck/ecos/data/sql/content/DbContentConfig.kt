package ru.citeck.ecos.data.sql.content

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig

@JsonDeserialize(builder = DbContentConfig.Builder::class)
data class DbContentConfig(
    val defaultContentStorage: EcosContentStorageConfig? = null
) {

    companion object {

        val DEFAULT = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbContentConfig {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder {

        var defaultContentStorage: EcosContentStorageConfig? = null

        fun withDefaultContentStorage(defaultContentStorage: EcosContentStorageConfig?): Builder {
            this.defaultContentStorage = defaultContentStorage
            return this
        }

        fun build(): DbContentConfig {
            return DbContentConfig(defaultContentStorage)
        }
    }
}
