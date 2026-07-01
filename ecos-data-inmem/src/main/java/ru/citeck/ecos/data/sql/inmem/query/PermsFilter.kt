package ru.citeck.ecos.data.sql.inmem.query

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource
import ru.citeck.ecos.data.sql.perms.DbPermsEntity
import ru.citeck.ecos.data.sql.records.DbRecordsUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.RecordConstants

/**
 * In-memory analogue of [ru.citeck.ecos.data.sql.pg.DbEntityRepoPg.getPermsCondition].
 *
 * A row is visible when there is a row in the `ed_read_perms` table whose `__entity_ref_id` equals
 * the row's perms column (`__ref_id` for OWN, the parent ref for PARENT) and whose `__authority_id`
 * is one of the current authorities. For the PARENT policy a row is also visible when no perms row
 * exists for the parent at all (PG's `OR NOT EXISTS(...)`). PUBLIC = no filter; NONE = no rows.
 *
 * Authorities come from [DbFindQuery.userAuthorities] (which the records DAO already populates with
 * the run-as authorities plus any non-type-scoped delegations); type-scoped delegated authorities
 * ([DbFindQuery.delegatedAuthorities]) widen visibility for records whose `__type` is one of the
 * delegated type ids - exactly the `OR (type IN (...) AND authority IN (...))` PG branch.
 */
class PermsFilter private constructor(
    val alwaysEmpty: Boolean,
    private val noFilter: Boolean,
    private val permsColumn: String,
    private val checkByParent: Boolean,
    private val authorityIds: Set<Long>,
    private val delegations: List<Pair<Set<Long>, Set<Long>>>,
    private val hasTypeColumn: Boolean,
    private val permsByRefId: Map<Long, Set<Long>>
) {

    fun test(row: Map<String, Any?>): Boolean {
        if (noFilter) {
            return true
        }
        if (alwaysEmpty) {
            return false
        }
        val refId = (row[permsColumn] as? Number)?.toLong()
        if (refId == null) {
            // PARENT policy: a record with no parent ref has no parent perms row -> visible
            return checkByParent
        }
        val auths = permsByRefId[refId] ?: return checkByParent
        if (auths.any { authorityIds.contains(it) }) {
            return true
        }
        if (hasTypeColumn && delegations.isNotEmpty()) {
            val typeId = (row[DbEntity.TYPE] as? Number)?.toLong()
            if (typeId != null) {
                for ((typeIds, delegationAuthIds) in delegations) {
                    if (typeIds.contains(typeId) && auths.any { delegationAuthIds.contains(it) }) {
                        return true
                    }
                }
            }
        }
        return false
    }

    companion object {

        fun create(context: DbTableContext, query: DbFindQuery): PermsFilter {
            val policy = context.getQueryPermsPolicy()
            val permsColumn = when (policy) {
                QueryPermsPolicy.PARENT -> RecordConstants.ATT_PARENT
                QueryPermsPolicy.OWN -> DbEntity.REF_ID
                QueryPermsPolicy.PUBLIC -> ""
                QueryPermsPolicy.NONE -> SEARCH_DISABLED_COLUMN
                else -> error("Invalid perms policy: $policy")
            }
            if (permsColumn.isBlank()) {
                return noFilter()
            }
            if (permsColumn == SEARCH_DISABLED_COLUMN || !context.hasColumn(permsColumn)) {
                return empty()
            }

            // mirror getPermsCondition: userAuthorities, else current run-as authorities
            val authorities = query.userAuthorities.ifEmpty { DbRecordsUtils.getCurrentAuthorities() }
            if (authorities.isEmpty()) {
                // mirror PG getPermsCondition: empty authorities -> no rows, checked BEFORE
                // delegations (a delegation never widens visibility for a blank run-as context)
                return empty()
            }
            val allAuthorityNames = HashSet(authorities)
            query.delegatedAuthorities.forEach { allAuthorityNames.addAll(it.second) }
            val authorityIdByName = context.getAuthoritiesIdsMap(allAuthorityNames)

            val authorityIds = authorities.mapNotNull { authorityIdByName[it] }.toHashSet()
            val delegations = query.delegatedAuthorities.mapNotNull { (typeIds, authNames) ->
                val authIds = authNames.mapNotNull { authorityIdByName[it] }.toHashSet()
                if (authIds.isEmpty() || typeIds.isEmpty()) null else (typeIds to authIds)
            }
            if (authorityIds.isEmpty() && delegations.isEmpty()) {
                return empty()
            }

            val ds = context.getDataSource() as? InMemDataSource
                ?: error("InMem perms filter requires an InMemDataSource")
            val permsTable = ds.getStore().getTable(context.getTableRef().withTable(DbPermsEntity.TABLE))
            val permsByRefId = HashMap<Long, MutableSet<Long>>()
            permsTable?.getRows()?.forEach { permRow ->
                val entityRefId = (permRow[DbPermsEntity.ENTITY_REF_ID] as? Number)?.toLong()
                val authorityId = (permRow[DbPermsEntity.AUTHORITY_ID] as? Number)?.toLong()
                if (entityRefId != null && authorityId != null) {
                    permsByRefId.getOrPut(entityRefId) { HashSet() }.add(authorityId)
                }
            }
            return PermsFilter(
                alwaysEmpty = false,
                noFilter = false,
                permsColumn = permsColumn,
                checkByParent = policy == QueryPermsPolicy.PARENT,
                authorityIds = authorityIds,
                delegations = delegations,
                hasTypeColumn = context.hasColumn(DbEntity.TYPE),
                permsByRefId = permsByRefId
            )
        }

        private fun noFilter() = PermsFilter(false, true, "", false, emptySet(), emptyList(), false, emptyMap())
        private fun empty() = PermsFilter(true, false, "", false, emptySet(), emptyList(), false, emptyMap())

        private const val SEARCH_DISABLED_COLUMN = "__search__disabled__"
    }
}
