package ru.citeck.ecos.data.sql.content.storage

import ecos.com.fasterxml.jackson210.annotation.JsonCreator
import ecos.com.fasterxml.jackson210.annotation.JsonValue

data class EcosContentDataUrl(
    val appName: String,
    val contentPath: String
) {
    companion object {
        const val SCHEMA = "ecd"
        const val LOCAL_STORAGE_APP_NAME = "local"

        private const val SCHEMA_PREFIX = "$SCHEMA://"

        @JvmStatic
        @JsonCreator
        fun valueOf(value: String?): EcosContentDataUrl {
            if (value == null || !value.startsWith(SCHEMA_PREFIX)) {
                error("Invalid URL: '$value'")
            }
            val nextDelimIdx = value.indexOf('/', startIndex = SCHEMA_PREFIX.length)
            if (nextDelimIdx == -1) {
                error("Invalid URL: '$value'")
            }
            val appName = value.substring(SCHEMA_PREFIX.length, nextDelimIdx)
            return create(appName, value.substring(nextDelimIdx + 1))
        }

        fun create(appName: String, path: String): EcosContentDataUrl {
            return EcosContentDataUrl(appName, path)
        }
    }

    fun isLocalStorageUrl(): Boolean {
        return appName == LOCAL_STORAGE_APP_NAME
    }

    @JsonValue
    override fun toString(): String {
        return "$SCHEMA://$appName/$contentPath"
    }
}
