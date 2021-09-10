package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.records2.RecordRef

interface DbPermsComponent {

    fun getAuthoritiesWithReadPermission(recordRef: RecordRef): Set<String>
}
