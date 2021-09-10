package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService

class CreatorPermsComponent(private val records: RecordsService) : DbPermsComponent {

    override fun getAuthoritiesWithReadPermission(recordRef: RecordRef): Set<String> {
        val creator = records.getAtt(recordRef, "_creator?localId").asText()
        if (creator.isNotBlank()) {
            return setOf(creator)
        }
        return emptySet()
    }
}
