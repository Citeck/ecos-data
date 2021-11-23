package ru.citeck.ecos.data.sql.service.migration

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.utils.ReflectUtils
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataService
import java.util.concurrent.ConcurrentHashMap

class DbMigrationService<T : Any>(
    val dataService: DbDataService<T>,
    val schemaDao: DbSchemaDao,
    val entityRepo: DbEntityRepo<T>,
    val dataSource: DbDataSource
) {

    private val migrations: MutableMap<String, DbMigration<T, Any>> = ConcurrentHashMap()

    fun runMigrationByType(type: String, mock: Boolean, config: ObjectData) {

        val migration = migrations[type]
            ?: error("Migration is not found: '$type'. Allowed values: ${migrations.keys.joinToString()}")

        val genericArgs = ReflectUtils.getGenericArgs(migration::class.java, DbMigration::class.java)
        if (genericArgs.size != 2) {
            error("Config type can't be resolved for " + migration::class)
        }
        val configType = genericArgs[1]
        val convertedConfig = Json.mapper.convert(config, configType)
            ?: error("Error while config conversion for " + migration::class)

        AuthContext.runAsSystem {
            migration.run(this, mock, convertedConfig)
        }
    }

    fun register(migration: DbMigration<*, *>) {
        @Suppress("UNCHECKED_CAST")
        migrations[migration.getType()] = migration as DbMigration<T, Any>
    }
}
