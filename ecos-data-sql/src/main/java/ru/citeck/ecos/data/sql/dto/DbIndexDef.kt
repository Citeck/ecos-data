package ru.citeck.ecos.data.sql.dto

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = DbIndexDef.Builder::class)
data class DbIndexDef(
    val name: String,
    val columns: List<String>,
    val unique: Boolean,
    val caseInsensitive: Boolean
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
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
        var caseInsensitive: Boolean = false

        constructor(base: DbIndexDef) : this() {
            name = base.name
            columns = base.columns
            unique = base.unique
            caseInsensitive = base.caseInsensitive
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

        fun withCaseInsensitive(caseInsensitive: Boolean?): Builder {
            this.caseInsensitive = caseInsensitive ?: false
            return this
        }

        fun build(): DbIndexDef {
            return DbIndexDef(name, columns, unique, caseInsensitive)
        }
    }
}
