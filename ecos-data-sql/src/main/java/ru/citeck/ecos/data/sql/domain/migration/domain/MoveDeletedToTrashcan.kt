package ru.citeck.ecos.data.sql.domain.migration.domain

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.domain.migration.DbDomainMigrationContext
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.trashcan.entity.DbTrashcanEntity
import java.sql.Array as SqlArray

class MoveDeletedToTrashcan : DbDomainMigration {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val BATCH_SIZE = 100
    }

    override fun run(context: DbDomainMigrationContext) {

        val dataSource = context.dataSource
        val tableRef = context.dataService.getTableRef()

        // Check if __deleted column exists
        val columns = context.schemaDao.getColumns(dataSource, tableRef)
        if (columns.none { it.name == DbEntity.DELETED }) {
            log.info { "Table ${tableRef.fullName} has no __deleted column, skipping migration" }
            return
        }

        // Ensure trashcan table is created
        val trashcanService = context.schemaContext.trashcanService
        trashcanService.createTableIfNotExists()

        val trashcanTableRef = context.schemaContext.getTableRef(DbTrashcanEntity.TABLE)

        // Check if ed_associations table exists for moving assocs
        val assocsTableRef = context.schemaContext.getTableRef("ed_associations")
        val assocsDeletedTableRef = context.schemaContext.getTableRef("ed_associations_deleted")
        val assocsColumns = context.schemaDao.getColumns(dataSource, assocsTableRef)
        val hasAssocsTable = assocsColumns.isNotEmpty()
        val hasDeletedAssocsTable = if (hasAssocsTable) {
            context.schemaDao.getColumns(dataSource, assocsDeletedTableRef).isNotEmpty()
        } else {
            false
        }

        log.info { "Starting migration of deleted records from ${tableRef.fullName} to trashcan." }

        var processed = 0L
        var lastId = -1L

        while (true) {

            // Fetch batch of deleted records
            val batch = dataSource.withTransaction(true) {
                dataSource.query(
                    "SELECT * FROM ${tableRef.fullName} " +
                        "WHERE \"${DbEntity.DELETED}\" IS TRUE AND \"${DbEntity.ID}\" > ? " +
                        "ORDER BY \"${DbEntity.ID}\" LIMIT $BATCH_SIZE",
                    listOf(lastId)
                ) { rs ->
                    val records = mutableListOf<Map<String, Any?>>()
                    val metaData = rs.metaData
                    val columnCount = metaData.columnCount
                    while (rs.next()) {
                        val record = LinkedHashMap<String, Any?>()
                        for (i in 1..columnCount) {
                            val value = rs.getObject(i)
                            // Convert SQL Array to Java List for proper JSON serialization
                            record[metaData.getColumnName(i)] = convertSqlValue(value)
                        }
                        records.add(record)
                    }
                    records
                }
            }

            if (batch.isEmpty()) {
                break
            }

            dataSource.withTransaction(false) {

                val idsToDelete = ArrayList<Long>(batch.size)
                val refIdsToMoveAssocs = ArrayList<Long>(batch.size)

                for (record in batch) {

                    val id = record[DbEntity.ID] as Long
                    lastId = id
                    idsToDelete.add(id)

                    val refId = (record[DbEntity.REF_ID] as? Number)?.toLong() ?: -1L
                    val modifier = (record[DbEntity.MODIFIER] as? Number)?.toLong() ?: -1L
                    val type = (record[DbEntity.TYPE] as? Number)?.toLong() ?: -1L
                    val name = record[DbEntity.NAME]?.let { Json.mapper.toString(it) } ?: "{}"

                    if (refId > 0) {
                        refIdsToMoveAssocs.add(refId)
                    }

                    val excludedKeys = setOf(
                        DbEntity.ID,
                        DbEntity.DELETED,
                        DbEntity.NAME,
                        DbEntity.REF_ID,
                        DbEntity.TYPE
                    )
                    val entityData = LinkedHashMap<String, Any?>()
                    record.forEach { (key, value) ->
                        if (key !in excludedKeys) {
                            entityData[key] = value
                        }
                    }

                    val entityDataJson = Json.mapper.toString(entityData) ?: "{}"

                    // Content IDs are not extracted during migration because type metadata
                    // (needed to identify CONTENT-type attributes) is not available in this context.
                    // The content data itself remains in ed_content and is not leaked.
                    dataSource.update(
                        "INSERT INTO ${trashcanTableRef.fullName} " +
                            "(\"${DbTrashcanEntity.REF_ID}\", " +
                            "\"${DbTrashcanEntity.SOURCE_TABLE}\", " +
                            "\"${DbTrashcanEntity.TYPE}\", " +
                            "\"${DbTrashcanEntity.NAME}\", " +
                            "\"${DbTrashcanEntity.DELETED_AT}\", " +
                            "\"${DbTrashcanEntity.DELETED_BY}\", " +
                            "\"${DbTrashcanEntity.DELETED_AS}\", " +
                            "\"${DbTrashcanEntity.TRACE_ID}\", " +
                            "\"${DbTrashcanEntity.TXN_ID}\", " +
                            "\"${DbTrashcanEntity.ENTITY_DATA}\", " +
                            "\"${DbTrashcanEntity.CONTENT_IDS}\") " +
                            "VALUES (?, ?, ?, ?::jsonb, NOW(), ?, ?, '', '', ?::jsonb, '{}'::bigint[])",
                        listOf(
                            refId,
                            tableRef.table,
                            type,
                            name,
                            modifier,
                            modifier,
                            entityDataJson
                        )
                    )
                }

                // Move associations of deleted records to ed_associations_deleted.
                // We exclude the 'id' column so that the deleted table generates new IDs.
                if (hasAssocsTable && hasDeletedAssocsTable && refIdsToMoveAssocs.isNotEmpty()) {
                    val assocDataColumns = assocsColumns
                        .filter { it.name != DbAssocEntity.ID }
                        .joinToString(",") { "\"${it.name}\"" }
                    if (assocDataColumns.isNotEmpty()) {
                        val refIdParam = refIdsToMoveAssocs.toTypedArray()
                        dataSource.update(
                            "INSERT INTO ${assocsDeletedTableRef.fullName} ($assocDataColumns) " +
                                "SELECT $assocDataColumns FROM ${assocsTableRef.fullName} " +
                                "WHERE \"${DbAssocEntity.SOURCE_ID}\" = ANY(?)",
                            listOf(refIdParam)
                        )
                        dataSource.update(
                            "DELETE FROM ${assocsTableRef.fullName} " +
                                "WHERE \"${DbAssocEntity.SOURCE_ID}\" = ANY(?)",
                            listOf(refIdParam)
                        )
                    }
                }

                // Bulk delete from source table
                dataSource.update(
                    "DELETE FROM ${tableRef.fullName} " +
                        "WHERE \"${DbEntity.ID}\" = ANY(?)",
                    listOf(idsToDelete.toTypedArray())
                )

                processed += batch.size
            }

            if (processed % 10_000 == 0L && processed > 0) {
                log.info { "Migrated $processed deleted records from ${tableRef.fullName}" }
            }
        }

        if (processed > 0) {
            log.info { "Migration completed. Moved $processed deleted records from ${tableRef.fullName} to trashcan." }
        } else {
            log.info { "No deleted records in ${tableRef.fullName}, nothing to migrate." }
        }
    }

    override fun getAppliedVersions(): Int {
        return 7
    }

    private fun convertSqlValue(value: Any?): Any? {
        if (value == null) {
            return null
        }
        if (value is SqlArray) {
            val array = value.array
            if (array is kotlin.Array<*>) {
                return array.map { convertSqlValue(it) }
            }
            return array
        }
        return value
    }
}
