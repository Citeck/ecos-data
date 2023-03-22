package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecPermsValue
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DefaultDbPermsComponent(
    private val recordsService: RecordsService
) : DbPermsComponent {

    companion object {
        private val AUTHORITIES_WITH_READ_PERMS = setOf(AuthGroup.EVERYONE)
    }

    override fun getEntityPerms(entityRef: EntityRef): DbRecordPerms {
        return DefaultRecPerms(recordsService.getAtts(entityRef, ParentPermsAtts::class.java))
    }

    private class ParentPermsAtts(
        @AttName("${RecordConstants.ATT_PARENT}.${DbRecord.ATT_PERMISSIONS}._has.Read?bool")
        val canRead: Boolean?,
        @AttName("${RecordConstants.ATT_PARENT}.${DbRecord.ATT_PERMISSIONS}._has.Write?bool")
        val canWrite: Boolean?,
        @AttName("${RecordConstants.ATT_PARENT}.${DbRecord.ATT_PERMISSIONS}.${DbRecPermsValue.ATT_AUTHORITIES_WITH_READ_PERMS}[]")
        val authoritiesWithReadPerms: List<String>?
    )

    private inner class DefaultRecPerms(private val parentPerms: ParentPermsAtts) : DbRecordPerms {

        override fun getAuthoritiesWithReadPermission(): Set<String> {
            return parentPerms.authoritiesWithReadPerms?.toSet() ?: AUTHORITIES_WITH_READ_PERMS
        }

        override fun isCurrentUserHasWritePerms(): Boolean {
            return parentPerms.canWrite ?: true
        }

        override fun isCurrentUserHasAttWritePerms(name: String): Boolean {
            return true
        }

        override fun isCurrentUserHasAttReadPerms(name: String): Boolean {
            return true
        }
    }
}
