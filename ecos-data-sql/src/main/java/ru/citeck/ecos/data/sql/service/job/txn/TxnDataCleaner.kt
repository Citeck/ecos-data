package ru.citeck.ecos.data.sql.service.job.txn

import mu.KotlinLogging
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.job.DbJob
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.txn.ExtTxnContext
import ru.citeck.ecos.records2.predicate.model.Predicates
import java.time.Instant

class TxnDataCleaner<T : Any>(
    private val mapper: DbEntityMapper<T>,
    private val txnDataService: DbDataService<T>,
    private val dbDataSource: DbDataSource,
    private val config: TxnDataCleanerConfig
) : DbJob {

    companion object {
        private const val TYPE_DELETED = "deleted"

        private val log = KotlinLogging.logger {}
    }

    override fun execute(): Boolean {
        return dbDataSource.withTransaction(false) {
            cleanTxnTable()
        }
    }

    /**
     * @return true if txn table has more entities to clean
     */
    private fun cleanTxnTable(): Boolean {

        log.debug { "Cleaning started for table ${txnDataService.getTableRef()}" }

        val entitiesToCleanFindRes = findStuckTxnEntities()
        val entitiesToClean = entitiesToCleanFindRes.entities
        if (entitiesToClean.isEmpty()) {
            return false
        }
        val tableRef = txnDataService.getTableRef()

        log.warn {
            "Found ${entitiesToClean.size} entities in transaction table $tableRef to clean. " +
                "Total count to clean: ${entitiesToCleanFindRes.totalCount}." +
                "This is an indicator of problem with the commit or rollback of the transactions"
        }

        for (entity in entitiesToClean) {
            val entityMap = LinkedHashMap(mapper.convertToMap(entity))
            val extIdBefore = entityMap[DbEntity.EXT_ID] as? String ?: continue
            val newExtId = extIdBefore + "-deleted-" + System.currentTimeMillis()
            entityMap[DbEntity.EXT_ID] = newExtId
            entityMap[DbEntity.TYPE] = TYPE_DELETED
            log.warn { "Update record '$extIdBefore' with new extId: '$newExtId'" }
            ExtTxnContext.withoutModifiedMeta {
                txnDataService.save(mapper.convertToEntity(entityMap))
            }
        }

        return entitiesToClean.size < entitiesToCleanFindRes.totalCount
    }

    private fun findStuckTxnEntities(): DbFindRes<T> {

        val lastAllowedRecModifiedTime = System.currentTimeMillis() - config.txnRecordLifeTimeMs

        return txnDataService.find(
            Predicates.and(
                Predicates.lt(
                    DbEntity.MODIFIED,
                    Instant.ofEpochMilli(lastAllowedRecModifiedTime)
                ),
                Predicates.not(
                    Predicates.eq(
                        DbEntity.TYPE,
                        TYPE_DELETED
                    )
                )
            ),
            listOf(
                DbFindSort(DbEntity.MODIFIED, true)
            ),
            DbFindPage(0, 50),
            withDeleted = true
        )
    }

    override fun getPeriod(): Long {
        return config.periodMs
    }

    override fun getInitDelay(): Long {
        return config.initDelayMs
    }

    fun getConfig(): TxnDataCleanerConfig {
        return config
    }

    fun getCleanerWithConfig(config: TxnDataCleanerConfig): TxnDataCleaner<T> {
        return TxnDataCleaner(mapper, txnDataService, dbDataSource, config)
    }
}
