package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.records3.record.atts.value.AttValue

class DbRecPermsValue(private val ctx: DbRecordsDaoCtx, private val recordId: String) : AttValue {

    private val recordPermsValue: DbRecordPerms? by lazy {
        if (!AuthContext.isRunAsSystem()) {
            ctx.recordsDao.getRecordPerms(recordId)
        } else {
            null
        }
    }

    fun getRecordPerms(): DbRecordPerms? {
        return recordPermsValue
    }

    override fun has(name: String): Boolean {
        val perms = recordPermsValue ?: return true
        if (name.equals("Write", true)) {
            return perms.isCurrentUserHasWritePerms()
        }
        if (name.equals("Read", true)) {
            val authoritiesWithReadPermissions = perms.getAuthoritiesWithReadPermission().toSet()
            if (AuthContext.getCurrentRunAsUserWithAuthorities().any { authoritiesWithReadPermissions.contains(it) }) {
                return true
            }
            return false
        }

        return super.has(name)
    }
}
