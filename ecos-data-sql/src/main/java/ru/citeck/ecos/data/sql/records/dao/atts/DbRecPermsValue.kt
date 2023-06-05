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

        const val PERMS_READ = "Read"
        const val PERMS_WRITE = "Write"
        const val PERMS_ALLOWED_ALL = "AllowedAll"
    }

    private val recordPermsValue: DbRecordPermsContext by lazy {
        ctx.recordsDao.getRecordPerms(record)
    }

    fun getRecordPerms(): DbRecordPermsContext {
        return recordPermsValue
    }

    override fun getAtt(name: String): Any? {
        return when (name) {
            ATT_AUTHORITIES_WITH_READ_PERMS -> recordPermsValue.getAuthoritiesWithReadPermission()
            ATT_ALL_ALLOWED_PERMS -> recordPermsValue.getAllowedPermissions()
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

        return perms.isAllowed(name)
    }
}
