package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DefaultDbPermsComponent : DbPermsComponent {

    companion object {
        private val AUTHORITIES_WITH_READ_PERMS = setOf(AuthGroup.EVERYONE)
    }

    override fun getRecordPerms(recordRef: EntityRef): DbRecordPerms {
        return DefaultRecPerms()
    }

    inner class DefaultRecPerms : DbRecordPerms {

        override fun getAuthoritiesWithReadPermission(): Set<String> {
            return AUTHORITIES_WITH_READ_PERMS
        }

        override fun isCurrentUserHasWritePerms(): Boolean {
            return true
        }

        override fun isCurrentUserHasAttWritePerms(name: String): Boolean {
            return true
        }

        override fun isCurrentUserHasAttReadPerms(name: String): Boolean {
            return true
        }
    }
}
