package ru.citeck.ecos.data.sql.meta.table.dto

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = DbTableMetaDto.Builder::class)
data class DbTableMetaDto(
    val id: String,
    val config: DbTableMetaConfig,
    val changelog: List<DbTableChangeSet>
) {
    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbTableMetaDto {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    fun copy(): Builder {
        return Builder(this)
    }

    class Builder() {

        var id: String = ""
        var config: DbTableMetaConfig = DbTableMetaConfig.DEFAULT
        var changelog: List<DbTableChangeSet> = emptyList()

        constructor(base: DbTableMetaDto) : this() {
            this.id = base.id
            this.config = base.config
            this.changelog = base.changelog
        }

        fun withId(id: String?): Builder {
            this.id = id ?: ""
            return this
        }

        fun withChangelog(changelog: List<DbTableChangeSet>?): Builder {
            this.changelog = changelog ?: emptyList()
            return this
        }

        fun withConfig(config: DbTableMetaConfig?): Builder {
            this.config = config ?: DbTableMetaConfig.DEFAULT
            return this
        }

        fun build(): DbTableMetaDto {
            return DbTableMetaDto(id, config, changelog)
        }
    }
}
