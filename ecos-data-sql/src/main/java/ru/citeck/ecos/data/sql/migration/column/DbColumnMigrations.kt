package ru.citeck.ecos.data.sql.migration.column

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.data.sql.batch.DbBatchTaskDto
import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog

internal object DbColumnMigrations {
    private val log = KotlinLogging.logger {}

    /** A failed worker can leave untransferred data. A newer task supersedes every older one. */
    fun findUnfinished(schema: DbSchemaContext, table: String): List<DbColumnMigrationParams> = schema.batchTaskService
        .findLatestUnfinishedByAttribute(table, DbColumnMigrationParams.HANDLER_TYPE)
        .mapNotNull { parse(it) }

    fun parse(task: DbBatchTaskDto): DbColumnMigrationParams? = try {
        DbColumnMigrationParams.from(task.params)
    } catch (e: Exception) {
        if (e is InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        }
        DbReadToleranceLog.warnOnce(log, "column-migration-params:${task.table}:${task.id}", e) {
            "Cannot read parameters of column migration ${task.id} in '${task.table}'; this task is ignored"
        }
        null
    }
}
