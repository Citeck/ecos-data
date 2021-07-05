package ru.citeck.ecos.data.sql.type

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.json.Json
import java.lang.reflect.Array
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

class DbTypesConverter {

    private val converters = ConcurrentHashMap<Pair<KClass<*>, KClass<*>>, (Any) -> Any?>()
    private val toCommonTypeConverters = ConcurrentHashMap<KClass<*>, (Any) -> Any?>()

    init {
        registerDefaultConverters()
    }

    fun <IN : Any> register(inCLass: KClass<IN>, converter: (IN) -> Any?) {
        @Suppress("UNCHECKED_CAST")
        this.toCommonTypeConverters[inCLass] = converter as (Any) -> Any?
    }

    fun <IN : Any, OUT : Any> register(inClass: KClass<IN>, outClass: KClass<OUT>, converter: (IN) -> OUT?) {
        @Suppress("UNCHECKED_CAST")
        this.converters[inClass to outClass] = converter as (Any) -> Any?
    }

    fun <T : Any> convert(valueIn: Any?, targetClass: KClass<T>): T? {

        valueIn ?: return null

        val toCommonTypeConverter = toCommonTypeConverters[valueIn::class]

        val value = if (toCommonTypeConverter != null) {
            toCommonTypeConverter.invoke(valueIn) ?: return null
        } else {
            valueIn
        }

        val result = if (targetClass.isInstance(value)) {
            value
        } else if (targetClass.java.isArray) {
            val targetComponentType = getComponentType(targetClass)
            if (value is Iterable<*>) {
                val resultList = value.map { convert(it, targetComponentType) }
                val result = Array.newInstance(targetComponentType.java, resultList.size)
                for (i in resultList.indices) {
                    Array.set(result, i, resultList[i])
                }
                result
            } else if (value::class.java.isArray) {
                val valueComponentClass = getComponentType(value::class)
                if (targetComponentType == valueComponentClass) {
                    value
                } else {
                    val length = Array.getLength(value)
                    val newArray = Array.newInstance(targetComponentType.java, length)
                    for (i in 0 until length) {
                        Array.set(newArray, i, convert(Array.get(value, i), targetComponentType))
                    }
                    newArray
                }
            } else {
                val newArray = Array.newInstance(targetComponentType.java, 1)
                Array.set(newArray, 0, convert(value, targetComponentType))
                newArray
            }
        } else {
            val conv = converters[value::class to targetClass]
                ?: error("Can't convert ${value::class} to $targetClass")
            conv.invoke(value)
        }
        return result as T?
    }

    private fun getComponentType(clazz: KClass<*>): KClass<*> {
        return clazz.java.componentType?.kotlin
            ?: error("Class is array, but component type null: $clazz")
    }

    private fun registerDefaultConverters() {
        register(Instant::class, Timestamp::class) { Timestamp.from(it) }
        register(Timestamp::class, Instant::class) { it.toInstant() }
        register(MLText::class, String::class) { Json.mapper.toString(it) ?: "" }
        register(String::class, MLText::class) { Json.mapper.read(it, MLText::class.java) ?: MLText.EMPTY }
    }
}
