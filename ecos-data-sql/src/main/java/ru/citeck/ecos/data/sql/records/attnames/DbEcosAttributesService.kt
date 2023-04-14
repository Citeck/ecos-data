package ru.citeck.ecos.data.sql.records.attnames

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate

class DbEcosAttributesService(
    schemaCtx: DbSchemaContext
) {

    private val dataService: DbDataService<DbEcosAttributeEntity> = DbDataServiceImpl(
        DbEcosAttributeEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbEcosAttributeEntity.TABLE)
        },
        schemaCtx
    )

    fun getAttsByIds(ids: Collection<Long>): Map<Long, String> {
        val entities = dataService.findAll(
            ValuePredicate(DbEcosAttributeEntity.ID, ValuePredicate.Type.IN, ids)
        )
        val attsByIds = entities.associate { it.id to it.extId }
        return toOrderedMap(ids, attsByIds, true)
    }

    fun getIdsForAtts(attributes: Collection<String>, createIfNotExists: Boolean): Map<String, Long> {

        val entities = dataService.findAll(Predicates.`in`(DbEcosAttributeEntity.EXT_ID, attributes))
        val idByExtId = HashMap<String, Long>(attributes.size)
        entities.forEach { idByExtId[it.extId] = it.id }

        if (createIfNotExists) {
            val newAtts = attributes.filter { !idByExtId.containsKey(it) }.map {
                val entity = DbEcosAttributeEntity()
                entity.extId = it
                entity
            }
            if (newAtts.isNotEmpty()) {
                dataService.save(newAtts).forEach {
                    idByExtId[it.extId] = it.id
                }
            }
        }
        return toOrderedMap(attributes, idByExtId, createIfNotExists)
    }

    private fun <K, V> toOrderedMap(
        keys: Collection<K>,
        valuesByKeys: Map<K, V>,
        allRequired: Boolean
    ): Map<K, V> {
        val result = LinkedHashMap<K, V>()
        keys.forEach {
            val id = valuesByKeys[it]
            if (id != null) {
                result[it] = id
            } else if (allRequired) {
                error("Key not found: $it")
            }
        }
        return result
    }
}
