package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthContext

class DbRecordPermsSystemAdapter(private val impl: DbRecordPerms) : DbRecordPerms {

    override fun getAuthoritiesWithReadPermission(): Set<String> {
        return AuthContext.runAsSystem { impl.getAuthoritiesWithReadPermission() }
    }

    override fun isCurrentUserHasWritePerms(): Boolean {
        return impl.isCurrentUserHasWritePerms()
    }

    override fun isCurrentUserHasAttWritePerms(name: String): Boolean {
        return impl.isCurrentUserHasAttWritePerms(name)
    }

    override fun isCurrentUserHasAttReadPerms(name: String): Boolean {
        return impl.isCurrentUserHasAttReadPerms(name)
    }
}
