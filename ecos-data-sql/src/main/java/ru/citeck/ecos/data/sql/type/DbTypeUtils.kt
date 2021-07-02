package ru.citeck.ecos.data.sql.type

import kotlin.reflect.KClass

object DbTypeUtils {

    fun getArrayType(elementType: KClass<*>): KClass<*> {
        return java.lang.reflect.Array.newInstance(elementType.java, 0)::class
    }
}
