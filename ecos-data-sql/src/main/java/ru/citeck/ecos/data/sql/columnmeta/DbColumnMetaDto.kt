package ru.citeck.ecos.data.sql.columnmeta

import java.time.Instant

/**
 * Immutable view of a [DbColumnMetaEntity] row.
 *
 * [attType] is non-null: downgrading the platform is forbidden in this product, so a row this
 * version cannot read is not a version-skew case to degrade from gracefully - it is a corrupted row
 * or a bug, and [DbColumnSemanticType.parse] throws rather than let it pass as "unknown".
 */
data class DbColumnMetaDto(
    val id: Long,
    val table: String,
    val columnName: String,
    val attId: String,
    val attType: DbColumnSemanticType,
    val multiple: Boolean,
    val backup: Boolean,
    val created: Instant,
    val creator: String
) {
    companion object {
        const val NEW_REC_ID = DbColumnMetaEntity.NEW_REC_ID
    }
}
