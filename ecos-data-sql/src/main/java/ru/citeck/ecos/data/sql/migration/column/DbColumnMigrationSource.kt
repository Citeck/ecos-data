package ru.citeck.ecos.data.sql.migration.column

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType

/** A physical source, independent of the target and lifecycle of the task using it. */
data class DbColumnMigrationSource(
    val backupColumn: String,
    val backupColumnMetaId: Long,
    val sourceType: DbColumnSemanticType,
    val sourceMultiple: Boolean,
    val sourceAssocBackup: Boolean = false
) {
    companion object {
        fun from(data: ObjectData): DbColumnMigrationSource = DbColumnMigrationSource(
            backupColumn = data.get("backupColumn").asText(),
            backupColumnMetaId = data.get("backupColumnMetaId").asLong(),
            sourceType = DbColumnSemanticType.parse(data.get("sourceType").asText()),
            sourceMultiple = data.get("sourceMultiple").asBoolean(),
            sourceAssocBackup = data.get("sourceAssocBackup").asBoolean(false)
        )
    }

    fun toObjectData(): ObjectData = ObjectData.create().also {
        it.set("backupColumn", backupColumn)
        it.set("backupColumnMetaId", backupColumnMetaId)
        it.set("sourceType", sourceType.asString())
        it.set("sourceMultiple", sourceMultiple)
        it.set("sourceAssocBackup", sourceAssocBackup)
    }
}
