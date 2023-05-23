package ru.citeck.ecos.data.sql.repo.entity.legacy

import kotlin.reflect.KClass

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
annotation class DbLegacyTypes(
    vararg val value: KClass<out DbLegacyEntity<*>>
)
