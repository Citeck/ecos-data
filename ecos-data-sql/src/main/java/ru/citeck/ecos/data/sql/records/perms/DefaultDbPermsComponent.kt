package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecPermsValue
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.RecordsService
import ru.citeck.ecos.records3.record.atts.schema.annotation.AttName
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DefaultDbPermsComponent(
    private val recordsService: RecordsService
) : DbPermsComponent {

    override fun getRecordPerms(user: String, authorities: Set<String>, record: Any): DbRecordPerms {
        val parentRef = AuthContext.runAsSystem {
            recordsService.getAtt(record, "${RecordConstants.ATT_PARENT}?id").asText().toEntityRef()
        }
        val parentAtts = if (EntityRef.isEmpty(parentRef)) {
            ParentPermsAtts(
                canRead = true,
                canWrite = true,
                authoritiesWithReadPerms = listOf(AuthGroup.EVERYONE),
                allAllowedPerms = listOf(DbRecPermsValue.PERMS_ALLOWED_ALL)
            )
        } else {
            AuthContext.runAs(user, authorities.toList()) {
                recordsService.getAtts(parentRef, ParentPermsAtts::class.java)
            }
        }
        return DefaultRecPerms(parentAtts)
    }

    private class ParentPermsAtts(
        @AttName("${DbRecord.ATT_PERMISSIONS}._has.${DbRecPermsValue.PERMS_READ}?bool!")
        val canRead: Boolean,

        @AttName("${DbRecord.ATT_PERMISSIONS}._has.${DbRecPermsValue.PERMS_WRITE}?bool!")
        val canWrite: Boolean,

        @AttName("${DbRecord.ATT_PERMISSIONS}.${DbRecPermsValue.ATT_AUTHORITIES_WITH_READ_PERMS}[]!")
        val authoritiesWithReadPerms: List<String>,

        @AttName("${DbRecord.ATT_PERMISSIONS}.${DbRecPermsValue.ATT_ALL_ALLOWED_PERMS}[]!")
        val allAllowedPerms: List<String>,
    )

    private inner class DefaultRecPerms(private val parentPerms: ParentPermsAtts) : DbRecordPerms {
        override fun isAllowed(permission: String): Boolean {
            val parentAllowedPerms = parentPerms.allAllowedPerms
            if (parentAllowedPerms.size == 1 && parentAllowedPerms.contains(DbRecPermsValue.PERMS_ALLOWED_ALL)) {
                return true
            }

            return parentPerms.allAllowedPerms.contains(permission)
        }

        override fun getAllowedPermissions(): Set<String> {
            return parentPerms.allAllowedPerms.toSet()
        }

        override fun getAuthoritiesWithReadPermission(): Set<String> {
            return parentPerms.authoritiesWithReadPerms.toSet()
        }

        override fun hasReadPerms(): Boolean {
            return parentPerms.canRead
        }

        override fun hasWritePerms(): Boolean {
            return parentPerms.canWrite
        }

        override fun hasAttWritePerms(name: String): Boolean {
            return true
        }

        override fun hasAttReadPerms(name: String): Boolean {
            return true
        }
    }
}
