package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.repo.entity.DbEntity

/**
 * Column resolution shared by every [DbEntityRepo] implementation of
 * [DbEntityRepo.updateByExtIdIfMatches]. The rules it enforces are part of that method's contract,
 * not of any one backend, so both maps of every backend have to be checked the same way - a backend
 * that resolved them differently would accept a call its peers reject.
 */
object DbConditionalUpdate {

    /**
     * Columns [DbEntityRepo.updateByExtIdIfMatches] maintains itself: the first two select the row
     * and the last one is incremented by the update.
     */
    val RESERVED_COLUMNS = setOf(DbEntity.ID, DbEntity.EXT_ID, DbEntity.UPD_VERSION)

    /**
     * Resolve the keys of [values] to table columns, preserving table order. An unknown key is an
     * error: dropping it silently would lose an assignment or, worse, drop a condition and let the
     * update apply to a row it was meant to skip. The identity and version columns are reserved -
     * they select the row and are maintained by the update itself.
     */
    fun getColumns(
        context: DbTableContext,
        values: Map<String, Any?>,
        valuesName: String
    ): List<DbColumnDef> {
        val reserved = values.keys.filter { RESERVED_COLUMNS.contains(it) }
        if (reserved.isNotEmpty()) {
            error("Reserved columns in $valuesName: $reserved. Table: ${context.getTableRef().fullName}")
        }
        val columns = context.getColumns().filter { values.containsKey(it.name) }
        if (columns.size != values.size) {
            val known = columns.mapTo(HashSet()) { it.name }
            error(
                "Unknown columns in $valuesName: ${values.keys.filter { !known.contains(it) }}. " +
                    "Table: ${context.getTableRef().fullName}"
            )
        }
        return columns
    }
}
