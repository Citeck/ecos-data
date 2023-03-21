package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbPermsComponent {

    // fun isInheritPermissionsForType(): Boolean

    fun getEntityPerms(entityRef: EntityRef): DbRecordPerms
}
