package ru.citeck.ecos.data.sql.dto

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = DbIndexDef.Builder::class)
data class DbIndexDef(
    val name: String,
    val columns: List<String>,
    val unique: Boolean
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        fun create(): Builder {
            return Builder()
        }

        fun create(builder: Builder.() -> Unit): DbIndexDef {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var name: String = ""
        var columns: List<String> = emptyList()
        var unique: Boolean = false

        constructor(base: DbIndexDef) : this() {
            columns = base.columns
            unique = base.unique
        }

        fun withName(name: String): Builder {
            this.name = name
            return this
        }

        fun withColumns(columns: List<String>?): Builder {
            this.columns = columns ?: EMPTY.columns
            return this
        }

        fun withUnique(unique: Boolean?): Builder {
            this.unique = unique ?: EMPTY.unique
            return this
        }

        fun build(): DbIndexDef {
            return DbIndexDef(name, columns, unique)
        }
    }
}
