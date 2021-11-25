package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.records2.RecordRef

data class DbRecordsDaoConfig(
    val id: String,
    val typeRef: RecordRef,
    val insertable: Boolean,
    val updatable: Boolean,
    val deletable: Boolean,
    val queryMaxItems: Int
) {

    companion object {

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbRecordsDaoConfig {
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
        var typeRef: RecordRef = RecordRef.EMPTY
        var insertable: Boolean = true
        var updatable: Boolean = true
        var deletable: Boolean = true
        var queryMaxItems: Int = 5000

        constructor(base: DbRecordsDaoConfig) : this() {
            this.id = base.id
            this.typeRef = base.typeRef
            this.insertable = base.insertable
            this.updatable = base.updatable
            this.deletable = base.deletable
            this.queryMaxItems = base.queryMaxItems
        }

        fun withId(id: String?): Builder {
            this.id = id ?: ""
            return this
        }

        fun withTypeRef(typeRef: RecordRef?): Builder {
            this.typeRef = typeRef ?: RecordRef.EMPTY
            return this
        }

        fun withInsertable(insertable: Boolean?): Builder {
            this.insertable = insertable ?: true
            return this
        }

        fun withUpdatable(updatable: Boolean?): Builder {
            this.updatable = updatable ?: true
            return this
        }

        fun withDeletable(deletable: Boolean?): Builder {
            this.deletable = deletable ?: true
            return this
        }

        fun withQueryMaxItems(queryMaxItems: Int?): Builder {
            this.queryMaxItems = queryMaxItems ?: 5000
            return this
        }

        fun build(): DbRecordsDaoConfig {
            return DbRecordsDaoConfig(
                id,
                typeRef,
                insertable,
                updatable,
                deletable,
                queryMaxItems
            )
        }
    }
}
