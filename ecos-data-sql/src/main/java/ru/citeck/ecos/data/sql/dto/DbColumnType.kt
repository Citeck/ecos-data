package ru.citeck.ecos.data.sql.dto

import java.time.Instant
import kotlin.reflect.KClass

enum class DbColumnType(val type: KClass<*>) {
    BIGSERIAL(Long::class),
    TEXT(String::class),
    DOUBLE(Double::class),
    INT(Integer::class),
    LONG(Long::class),
    BOOLEAN(Boolean::class),
    DATETIME(Instant::class),
    JSON(String::class),
    BINARY(ByteArray::class),
    UUID(java.util.UUID::class);
}
