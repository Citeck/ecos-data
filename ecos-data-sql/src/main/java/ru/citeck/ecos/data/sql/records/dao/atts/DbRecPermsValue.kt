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
            else -> null
        }
    }

    override fun has(name: String): Boolean {
        val perms = recordPermsValue
        if (name.equals("Write", true)) {
            return perms.hasWritePerms()
        }
        if (name.equals("Read", true)) {
            return perms.hasReadPerms()
        }
        return super.has(name)
    }
}
