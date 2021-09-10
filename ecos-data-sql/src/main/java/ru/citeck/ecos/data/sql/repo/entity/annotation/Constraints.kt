package ru.citeck.ecos.data.sql.repo.entity.annotation

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint

@Retention(AnnotationRetention.RUNTIME)
@Target(
    AnnotationTarget.FIELD,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY_GETTER,
    AnnotationTarget.PROPERTY_SETTER
)
annotation class Constraints(
    vararg val value: DbColumnConstraint
)
