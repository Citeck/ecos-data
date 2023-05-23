package ru.citeck.ecos.data.sql.repo.entity.legacy

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import java.time.Instant

class DbLegacyEntity0 : DbLegacyEntity<DbEntity> {

    companion object {
        const val MAX_SCHEMA_VERSION = 3
    }

    var id: Long = DbEntity.NEW_REC_ID
    var refId: Long = DbEntity.NEW_REC_ID
    var extId: String = ""
    var updVersion: Long = 0
    var modified: Instant = Instant.EPOCH
    var modifier: String = ""
    var created: Instant = Instant.EPOCH
    var creator: String = ""
    var deleted: Boolean = false
    var type: String = ""
    var status: String = ""
    var name: MLText = MLText.EMPTY

    var attributes: MutableMap<String, Any?> = LinkedHashMap()

    override fun getAsEntity(): DbEntity {

        val entity = DbEntity()
        entity.id = id
        entity.refId = refId
        entity.extId = extId
        entity.updVersion = updVersion
        entity.modified = modified
        entity.modifier = -1
        entity.created = created
        entity.creator = -1
        entity.deleted = deleted
        entity.type = -1
        entity.legacyType = type
        entity.status = status
        entity.name

        entity.attributes.putAll(attributes)

        return entity
    }
}
