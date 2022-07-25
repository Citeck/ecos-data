package ru.citeck.ecos.data.sql.repo.entity.annotation

@Retention(AnnotationRetention.RUNTIME)
annotation class Index(
    vararg val columns: String,
    val unique: Boolean = false,
    val caseInsensitive: Boolean = false
)
