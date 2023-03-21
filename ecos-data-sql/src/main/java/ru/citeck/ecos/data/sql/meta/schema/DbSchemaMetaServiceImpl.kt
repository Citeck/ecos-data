package ru.citeck.ecos.data.sql.meta.schema

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.NameUtils
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl

class DbSchemaMetaServiceImpl : DbSchemaMetaService {

    companion object {
        private val SCOPE_PART_NAME_ESCAPER = NameUtils.getEscaper(".")
    }

    private val scope: String
    private val dataService: DbDataService<DbSchemaMetaEntity>

    constructor(schemaCtx: DbSchemaContext) : this(
        "",
        DbDataServiceImpl(
            DbSchemaMetaEntity::class.java,
            DbDataServiceConfig.create()
                .withTable(DbSchemaMetaEntity.TABLE)
                .build(),
            schemaCtx
        )
    )

    private constructor(scope: String, dataService: DbDataService<DbSchemaMetaEntity>) {
        this.scope = scope
        this.dataService = dataService
    }

    override fun getScoped(scope: String): DbSchemaMetaService {

        if (scope.isEmpty()) {
            return this
        }

        if (scope == DbSchemaMetaService.ROOT_SCOPE) {
            return DbSchemaMetaServiceImpl("", dataService)
        }
        val newScope = if (this.scope.isEmpty()) {
            scope
        } else {
            this.scope + "." + scope
        }
        return DbSchemaMetaServiceImpl(newScope, dataService)
    }

    override fun <T : Any> getValue(key: List<String>, orElse: T): T {
        return getValue(key.joinToString(".") { SCOPE_PART_NAME_ESCAPER.escape(it) }, orElse)
    }

    override fun <T : Any> getValue(key: String, orElse: T): T {
        return getValue(key).getAs(orElse::class.java) ?: orElse
    }

    override fun getValue(key: List<String>): DataValue {
        return getValue(key.joinToString(".") { SCOPE_PART_NAME_ESCAPER.escape(it) })
    }

    override fun getValue(key: String): DataValue {
        val entity = dataService.findByExtId(getScopedKey(key)) ?: return DataValue.NULL
        return Json.mapper.readData(entity.value)
    }

    override fun setValue(key: List<String>, value: Any?) {
        setValue(key.joinToString(".") { SCOPE_PART_NAME_ESCAPER.escape(it) }, value)
    }

    override fun setValue(key: String, value: Any?) {
        val scopedKey = getScopedKey(key)
        val entity = dataService.findByExtId(scopedKey) ?: run {
            val newEntity = DbSchemaMetaEntity()
            newEntity.extId = scopedKey
            newEntity
        }
        entity.value = DataValue.create(value).toString()
        dataService.save(entity)
    }

    private fun getScopedKey(key: String): String {
        return if (scope.isEmpty()) {
            key
        } else {
            "$scope.$key"
        }
    }
}
