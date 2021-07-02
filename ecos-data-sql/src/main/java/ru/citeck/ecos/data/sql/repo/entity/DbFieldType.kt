package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.ecos.data.sql.dto.DbColumnType

@Retention(AnnotationRetention.RUNTIME)
@Target(
    AnnotationTarget.FIELD,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY_GETTER,
    AnnotationTarget.PROPERTY_SETTER
)
annotation class DbFieldType(
    val value: DbColumnType
)
