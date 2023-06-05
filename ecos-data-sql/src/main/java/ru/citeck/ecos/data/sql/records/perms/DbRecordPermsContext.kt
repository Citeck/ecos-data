package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.context.lib.auth.AuthContext

class DbRecordPermsContext(
    private val perms: DbRecordPerms
) {

    fun getAuthoritiesWithReadPermission(): Set<String> {
        return AuthContext.runAsSystem { perms.getAuthoritiesWithReadPermission() }
    }

    fun hasReadPerms(): Boolean {
        return AuthContext.runAsSystem { perms.hasReadPerms() }
    }

    fun hasWritePerms(): Boolean {
        return AuthContext.runAsSystem { perms.hasWritePerms() }
    }

    fun hasAttWritePerms(name: String): Boolean {
        return AuthContext.runAsSystem { perms.hasAttWritePerms(name) }
    }

    fun hasAttReadPerms(name: String): Boolean {
        return AuthContext.runAsSystem { perms.hasAttReadPerms(name) }
    }

    fun isAllowed(permission: String): Boolean {
        return AuthContext.runAsSystem { perms.isAllowed(permission) }
    }

    fun getAllowedPermissions(): Set<String> {
        return AuthContext.runAsSystem { perms.getAllowedPermissions() }
    }
}
