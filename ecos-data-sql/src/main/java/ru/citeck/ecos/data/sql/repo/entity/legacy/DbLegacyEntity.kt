package ru.citeck.ecos.data.sql.repo.entity.legacy

interface DbLegacyEntity<T : Any> {

    companion object {
        const val MAX_SCHEMA_VERSION_FIELD_NAME = "MAX_SCHEMA_VERSION"
    }

    fun getAsEntity(): T
}
