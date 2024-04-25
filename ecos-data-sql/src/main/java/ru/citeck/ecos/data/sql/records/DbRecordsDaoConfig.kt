package ru.citeck.ecos.data.sql.records

import ru.citeck.ecos.webapp.api.entity.EntityRef

data class DbRecordsDaoConfig(
    val id: String,
    val typeRef: EntityRef,
    val insertable: Boolean,
    val updatable: Boolean,
    val deletable: Boolean,
    val queryMaxItems: Int,
    val authEnabled: Boolean,
    val enableTotalCount: Boolean
) {

    companion object {

        private val VALID_ID_PATTERN = "[\\w-]+".toRegex()

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
        var typeRef: EntityRef = EntityRef.EMPTY
        var insertable: Boolean = true
        var updatable: Boolean = true
        var deletable: Boolean = true
        var queryMaxItems: Int = 10000
        var authEnabled: Boolean = false
        var enableTotalCount: Boolean = true

        constructor(base: DbRecordsDaoConfig) : this() {
            this.id = base.id
            this.typeRef = base.typeRef
            this.insertable = base.insertable
            this.updatable = base.updatable
            this.deletable = base.deletable
            this.queryMaxItems = base.queryMaxItems
            this.authEnabled = base.authEnabled
            this.enableTotalCount = base.enableTotalCount
        }

        fun withId(id: String?): Builder {
            this.id = id ?: ""
            return this
        }

        fun withTypeRef(typeRef: EntityRef?): Builder {
            this.typeRef = typeRef ?: EntityRef.EMPTY
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
            this.queryMaxItems = queryMaxItems ?: 10000
            return this
        }

        fun withAuthEnabled(authEnabled: Boolean?): Builder {
            this.authEnabled = authEnabled ?: false
            return this
        }

        fun withEnableTotalCount(enableTotalCount: Boolean?): Builder {
            this.enableTotalCount = enableTotalCount ?: true
            return this
        }

        fun build(): DbRecordsDaoConfig {
            if (!VALID_ID_PATTERN.matches(id)) {
                error("Invalid records DAO id - '$id'. Valid pattern: $VALID_ID_PATTERN")
            }
            return DbRecordsDaoConfig(
                id,
                typeRef,
                insertable,
                updatable,
                deletable,
                queryMaxItems,
                authEnabled,
                enableTotalCount
            )
        }
    }
}
