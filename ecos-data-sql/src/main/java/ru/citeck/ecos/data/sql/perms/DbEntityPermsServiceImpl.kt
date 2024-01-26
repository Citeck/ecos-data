package ru.citeck.ecos.data.sql.perms

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.dto.fk.DbFkConstraint
import ru.citeck.ecos.data.sql.dto.fk.FkCascadeActionOptions
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.auth.DbAuthorityEntity
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import ru.citeck.ecos.txn.lib.TxnContext
import java.util.HashSet
import kotlin.system.measureTimeMillis

class DbEntityPermsServiceImpl(private val schemaCtx: DbSchemaContext) : DbEntityPermsService {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val dataSource = schemaCtx.dataSourceCtx.dataSource
    private val dataService: DbDataService<DbPermsEntity>
    private val authorityService = schemaCtx.authorityDataService

    init {
        dataService = DbDataServiceImpl(
            DbPermsEntity::class.java,
            DbDataServiceConfig.create()
                .withTable(DbPermsEntity.TABLE)
                .withFkConstraints(
                    listOf(
                        DbFkConstraint.create {
                            withName("fk_" + DbPermsEntity.TABLE + "_authority_id")
                            withBaseColumnName(DbPermsEntity.AUTHORITY_ID)
                            withReferencedTable(DbTableRef(schemaCtx.schema, DbAuthorityEntity.TABLE))
                            withReferencedColumn(DbAuthorityEntity.ID)
                            withOnDelete(FkCascadeActionOptions.CASCADE)
                        },
                        DbFkConstraint.create {
                            withName("fk_" + DbRecordRefEntity.TABLE + "_entity_ref_id")
                            withBaseColumnName(DbPermsEntity.ENTITY_REF_ID)
                            withReferencedTable(DbTableRef(schemaCtx.schema, DbRecordRefEntity.TABLE))
                            withReferencedColumn(DbEntity.ID)
                            withOnDelete(FkCascadeActionOptions.CASCADE)
                        }
                    )
                )
                .build(),
            schemaCtx
        )
    }

    override fun createTableIfNotExists() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
        }
    }

    override fun setReadPerms(permissions: List<DbEntityPermsDto>) {
        val time = measureTimeMillis {
            dataSource.withTransaction(false) {
                setReadPermsImpl(permissions)
            }
        }

        log.trace { "Set read permissions for <$permissions> in $time ms" }
    }

    private fun setReadPermsImpl(permissions: List<DbEntityPermsDto>) {
        val allAuthorities = mutableSetOf<String>()
        permissions.forEach {
            allAuthorities.addAll(it.readAllowed)
        }
        setReadPermsInDb(permissions, ensureAuthoritiesExists(allAuthorities))
    }

    private fun setReadPermsInDb(permissions: List<DbEntityPermsDto>, authorityIdByName: Map<String, Long>) {

        for (entityPerms in permissions) {

            val entityRefId = entityPerms.entityRefId

            val currentAllowedPerms: List<DbPermsEntity> = dataService.findAll(
                Predicates.eq(DbPermsEntity.ENTITY_REF_ID, entityRefId)
            )

            val allowedAuth = HashSet(
                entityPerms.readAllowed.map {
                    authorityIdByName[it] ?: error("Authority id doesn't found for '$it'")
                }
            )

            val allowedPermsAuthToDelete = currentAllowedPerms.filter {
                !allowedAuth.contains(it.authorityId)
            }.map {
                it.authorityId
            }
            if (allowedPermsAuthToDelete.isNotEmpty()) {
                dataService.forceDelete(
                    Predicates.and(
                        Predicates.eq(DbPermsEntity.ENTITY_REF_ID, entityRefId),
                        ValuePredicate(DbPermsEntity.AUTHORITY_ID, ValuePredicate.Type.IN, allowedPermsAuthToDelete)
                    )
                )
            }

            for (perms in currentAllowedPerms) {
                allowedAuth.remove(perms.authorityId)
            }

            if (allowedAuth.isEmpty()) {
                continue
            }

            val entitiesToSave = ArrayList<DbPermsEntity>(allowedAuth.size)

            allowedAuth.forEach {
                val entity = DbPermsEntity()
                entity.entityRefId = entityRefId
                entity.authorityId = it
                entitiesToSave.add(entity)
            }

            dataService.save(entitiesToSave)
        }
    }

    private fun ensureAuthoritiesExists(authorities: Set<String>): Map<String, Long> {

        if (authorities.isEmpty()) {
            return emptyMap()
        }

        val authorityEntities = authorityService.findAll(
            Predicates.`in`(DbAuthorityEntity.EXT_ID, authorities)
        )
        val authoritiesId = mutableMapOf<String, Long>()

        for (authEntity in authorityEntities) {
            authoritiesId[authEntity.extId] = authEntity.id
        }

        for (authority in authorities) {
            if (!authoritiesId.containsKey(authority)) {
                val authEntity = DbAuthorityEntity()
                authEntity.extId = authority
                authoritiesId[authority] = authorityService.save(authEntity, emptyList()).id
            }
        }

        return authoritiesId
    }

    override fun isTableExists(): Boolean {
        return dataService.isTableExists()
    }

    override fun resetColumnsCache() {
        return dataService.resetColumnsCache()
    }
}
