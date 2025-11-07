package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.context.lib.auth.data.SimpleAuthData
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
            val typeIsSystem = AuthContext.runAsSystem {
                recordsService.getAtt(record, "${RecordConstants.ATT_TYPE}.system?bool!").asBoolean()
            }
            val userAuthData = SimpleAuthData(user, authorities.toList())
            val canWrite = (AuthContext.isSystemAuth(userAuthData) || AuthContext.isAdminAuth(userAuthData)) ||
                typeIsSystem.not()

            PermsAtts(
                canRead = true,
                canWrite = canWrite,
                authoritiesWithReadPerms = listOf(AuthGroup.EVERYONE),
                additionalPerms = listOf(DbRecPermsValue.ADDITIONAL_PERMS_ALL)
            )
        } else {
            AuthContext.runAs(user, authorities.toList()) {
                recordsService.getAtts(parentRef, PermsAtts::class.java)
            }
        }
        return DefaultRecPerms(parentAtts)
    }

    private class PermsAtts(
        @AttName("${DbRecord.ATT_PERMISSIONS}._has.${DbRecPermsValue.PERMS_READ}?bool!")
        val canRead: Boolean,

        @AttName("${DbRecord.ATT_PERMISSIONS}._has.${DbRecPermsValue.PERMS_WRITE}?bool!")
        val canWrite: Boolean,

        @AttName("${DbRecord.ATT_PERMISSIONS}.${DbRecPermsValue.ATT_AUTHORITIES_WITH_READ_PERMS}[]!")
        val authoritiesWithReadPerms: List<String>,

        @AttName("${DbRecord.ATT_PERMISSIONS}.${DbRecPermsValue.ATT_ADDITIONAL_PERMS}[]!")
        val additionalPerms: List<String>,
    )

    private inner class DefaultRecPerms(private val permsAtts: PermsAtts) : DbRecordPerms {

        override fun getAdditionalPerms(): Set<String> {
            return permsAtts.additionalPerms.toSet()
        }

        override fun getAuthoritiesWithReadPermission(): Set<String> {
            return permsAtts.authoritiesWithReadPerms.toSet()
        }

        override fun hasReadPerms(): Boolean {
            return permsAtts.canRead
        }

        override fun hasWritePerms(): Boolean {
            return permsAtts.canWrite
        }

        override fun hasAttWritePerms(name: String): Boolean {
            return permsAtts.canWrite
        }

        override fun hasAttReadPerms(name: String): Boolean {
            return permsAtts.canRead
        }
    }
}
