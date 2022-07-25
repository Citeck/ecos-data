package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.records.perms.DbRecordPerms
import ru.citeck.ecos.records3.record.atts.value.AttEdge

class DbRecordAttEdge(
    private val rec: DbRecord,
    private val name: String,
    private val perms: DbRecordPerms?
) : AttEdge {

    override fun getName(): String {
        return name
    }

    override fun getValue(): Any? {
        return rec.getAtt(name)
    }

    override fun isProtected(): Boolean {
        return perms != null && !perms.isCurrentUserHasAttWritePerms(name)
    }

    override fun isUnreadable(): Boolean {
        return perms != null && !perms.isCurrentUserHasAttReadPerms(name)
    }
}
