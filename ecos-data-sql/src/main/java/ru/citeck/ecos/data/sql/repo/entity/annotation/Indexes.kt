package ru.citeck.ecos.data.sql.repo.entity.annotation

@Retention(AnnotationRetention.RUNTIME)
@Target(
    AnnotationTarget.CLASS,
)
annotation class Indexes(
    vararg val value: Index
)
