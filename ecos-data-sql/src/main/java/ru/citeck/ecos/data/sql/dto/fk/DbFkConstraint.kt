package ru.citeck.ecos.data.sql.dto.fk

import ecos.com.fasterxml.jackson210.databind.annotation.JsonDeserialize
import ru.citeck.ecos.data.sql.dto.DbTableRef

@JsonDeserialize(builder = DbFkConstraint.Builder::class)
data class DbFkConstraint(
    val name: String,
    val baseColumnName: String,
    val referencedColumn: String,
    val referencedTable: DbTableRef,
    val onDelete: FkCascadeActionOptions = FkCascadeActionOptions.NO_ACTION,
    val onUpdate: FkCascadeActionOptions = FkCascadeActionOptions.NO_ACTION
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbFkConstraint {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var name: String = ""
        var baseColumnName: String = ""
        var referencedColumn: String = ""
        var referencedTable: DbTableRef = DbTableRef.EMPTY
        var onDelete: FkCascadeActionOptions = FkCascadeActionOptions.NO_ACTION
        var onUpdate: FkCascadeActionOptions = FkCascadeActionOptions.NO_ACTION

        constructor(base: DbFkConstraint) : this() {
            name = base.name
            baseColumnName = base.baseColumnName
            referencedColumn = base.referencedColumn
            referencedTable = base.referencedTable
            onDelete = base.onDelete
            onUpdate = base.onUpdate
        }

        fun withName(name: String?): Builder {
            this.name = name ?: ""
            return this
        }

        fun withBaseColumnName(baseColumnName: String): Builder {
            this.baseColumnName = baseColumnName
            return this
        }

        fun withReferencedColumn(referencedColumn: String): Builder {
            this.referencedColumn = referencedColumn
            return this
        }

        fun withReferencedTable(referencedTable: DbTableRef): Builder {
            this.referencedTable = referencedTable
            return this
        }

        fun withOnDelete(onDelete: FkCascadeActionOptions): Builder {
            this.onDelete = onDelete
            return this
        }

        fun withOnUpdate(onUpdate: FkCascadeActionOptions): Builder {
            this.onUpdate = onUpdate
            return this
        }

        fun build(): DbFkConstraint {
            return DbFkConstraint(
                name,
                baseColumnName,
                referencedColumn,
                referencedTable,
                onDelete,
                onUpdate
            )
        }
    }
}
