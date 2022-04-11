package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.records2.RecordRef

interface DbPermsComponent {

    fun getRecordPerms(recordRef: RecordRef): DbRecordPerms
}
