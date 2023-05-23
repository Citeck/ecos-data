package ru.citeck.ecos.data.sql.perms

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

class DbEntityPermsServiceImpl(private val schemaCtx: DbSchemaContext) : DbEntityPermsService {

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
        dataSource.withTransaction(false) {
            setReadPermsImpl(permissions)
        }
    }

    private fun setReadPermsImpl(permissions: List<DbEntityPermsDto>) {
        val allAuthorities = mutableSetOf<String>()
        permissions.forEach {
            allAuthorities.addAll(it.readAllowed)
            allAuthorities.addAll(it.readDenied)
        }
        setReadPermsInDb(permissions, ensureAuthoritiesExists(allAuthorities))
    }

    private fun setReadPermsInDb(permissions: List<DbEntityPermsDto>, authorityIdByName: Map<String, Long>) {

        for (entityPerms in permissions) {

            val entityRefId = entityPerms.entityRefId

            val currentPerms: List<DbPermsEntity> = dataService.findAll(
                Predicates.eq(DbPermsEntity.ENTITY_REF_ID, entityRefId)
            )

            val allowedAuth = HashSet(
                entityPerms.readAllowed.map {
                    authorityIdByName[it] ?: error("Authority id doesn't found for '$it'")
                }
            )
            val deniedAuth = HashSet(
                entityPerms.readDenied.map {
                    authorityIdByName[it] ?: error("Authority id doesn't found for '$it'")
                }
            )

            val permsAuthToDelete = currentPerms.filter {
                if (it.allowed) {
                    !allowedAuth.contains(it.authorityId)
                } else {
                    !deniedAuth.contains(it.authorityId)
                }
            }.map {
                it.authorityId
            }
            if (permsAuthToDelete.isNotEmpty()) {
                dataService.forceDelete(
                    Predicates.and(
                        Predicates.eq(DbPermsEntity.ENTITY_REF_ID, entityRefId),
                        ValuePredicate(DbPermsEntity.AUTHORITY_ID, ValuePredicate.Type.IN, permsAuthToDelete)
                    )
                )
            }

            for (perms in currentPerms) {
                if (perms.allowed) {
                    allowedAuth.remove(perms.authorityId)
                } else {
                    deniedAuth.remove(perms.authorityId)
                }
            }

            if (allowedAuth.isEmpty() && deniedAuth.isEmpty()) {
                return
            }

            val entitiesToSave = ArrayList<DbPermsEntity>(allowedAuth.size + deniedAuth.size)
            fun addEntityToSave(allowed: Boolean, authorityId: Long) {
                val entity = DbPermsEntity()
                entity.entityRefId = entityRefId
                entity.authorityId = authorityId
                entity.allowed = allowed
                entitiesToSave.add(entity)
            }

            allowedAuth.forEach { addEntityToSave(true, it) }
            deniedAuth.forEach { addEntityToSave(false, it) }

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
