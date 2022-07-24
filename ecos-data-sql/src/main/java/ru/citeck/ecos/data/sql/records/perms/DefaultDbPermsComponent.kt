package ru.citeck.ecos.data.sql.records.perms

import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.RecordsService

class DefaultDbPermsComponent(private val records: RecordsService) : DbPermsComponent {

    override fun getRecordPerms(recordRef: RecordRef): DbRecordPerms {
        return CreatorRecPerms(recordRef)
    }

    inner class CreatorRecPerms(val recordRef: RecordRef) : DbRecordPerms {

        override fun getAuthoritiesWithReadPermission(): Set<String> {
            val creator = records.getAtt(recordRef, "_creator?localId").asText()
            if (creator.isNotBlank()) {
                return setOf(creator)
            }
            return emptySet()
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
