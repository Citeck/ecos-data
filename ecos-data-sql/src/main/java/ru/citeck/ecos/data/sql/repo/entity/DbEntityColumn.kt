package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.ecos.data.sql.dto.DbColumnDef
import kotlin.reflect.KClass

class DbEntityColumn(
    val fieldName: String,
    val fieldType: KClass<*>,
    val defaultValue: Any?,
    val columnDef: DbColumnDef,
    val getter: (Any) -> Any?
)
