package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.perms.DbRecordPermsContext
import ru.citeck.ecos.records3.record.atts.value.AttValue

class DbRecPermsValue(
    private val ctx: DbRecordsDaoCtx,
    private val record: DbRecord
) : AttValue {

    companion object {
        const val ATT_AUTHORITIES_WITH_READ_PERMS = "authoritiesWithReadPerms"
        const val ATT_ALL_ALLOWED_PERMS = "allAllowedPerms"
        const val ATT_ADDITIONAL_PERMS = "additionalPerms"

        const val PERMS_READ = "Read"
        const val PERMS_WRITE = "Write"

        const val ADDITIONAL_PERMS_ALL = "ALL"
    }

    private val recordPermsValue: DbRecordPermsContext by lazy {
        ctx.recordsDao.getRecordPerms(record)
    }
    private val additionalPermsUpper: Set<String> by lazy {
        recordPermsValue.getAdditionalPerms().mapTo(HashSet()) { it.uppercase() }
    }

    fun getRecordPerms(): DbRecordPermsContext {
        return recordPermsValue
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            ATT_AUTHORITIES_WITH_READ_PERMS -> recordPermsValue.getAuthoritiesWithReadPermission()
            ATT_ADDITIONAL_PERMS -> recordPermsValue.getAdditionalPerms()
            ATT_ALL_ALLOWED_PERMS -> {
                val perms = LinkedHashSet<String>()
                if (recordPermsValue.hasReadPerms()) {
                    perms.add(PERMS_READ)
                }
                if (recordPermsValue.hasWritePerms()) {
                    perms.add(PERMS_WRITE)
                }
                perms.addAll(recordPermsValue.getAdditionalPerms())
                perms
            }
            else -> null
        }
    }

    override fun has(name: String): Boolean {
        val perms = recordPermsValue
        if (name.equals(PERMS_WRITE, true)) {
            return perms.hasWritePerms()
        }
        if (name.equals(PERMS_READ, true)) {
            return perms.hasReadPerms()
        }
        return additionalPermsUpper.contains(ADDITIONAL_PERMS_ALL) ||
            additionalPermsUpper.contains(name.uppercase())
    }
}
