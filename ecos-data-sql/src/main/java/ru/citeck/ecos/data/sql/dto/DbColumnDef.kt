package ru.citeck.ecos.data.sql.dto

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = DbColumnDef.Builder::class)
data class DbColumnDef(
    val name: String,
    val type: DbColumnType,
    val multiple: Boolean,
    val constraints: List<DbColumnConstraint>
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbColumnDef {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    fun withConstraints(constraints: List<DbColumnConstraint>): DbColumnDef {
        return Builder(this)
            .withConstraints(constraints)
            .build()
    }

    class Builder() {

        var name: String = ""
        var type: DbColumnType = DbColumnType.TEXT
        var multiple: Boolean = false
        var constraints: List<DbColumnConstraint> = emptyList()

        constructor(base: DbColumnDef) : this() {
            name = base.name
            type = base.type
            multiple = base.multiple
            constraints = base.constraints
        }

        fun withName(name: String?): Builder {
            this.name = name ?: ""
            return this
        }

        fun withType(type: DbColumnType?): Builder {
            this.type = type ?: DbColumnType.TEXT
            return this
        }

        fun withMultiple(multiple: Boolean?): Builder {
            this.multiple = multiple ?: false
            return this
        }

        fun withConstraints(constraints: List<DbColumnConstraint>?): Builder {
            this.constraints = constraints ?: emptyList()
            return this
        }

        fun build(): DbColumnDef {
            return DbColumnDef(
                name,
                type,
                multiple,
                constraints
            )
        }
    }
}
