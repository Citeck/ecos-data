package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import ru.citeck.ecos.data.sql.repo.entity.legacy.DbLegacyEntity0
import ru.citeck.ecos.data.sql.repo.entity.legacy.DbLegacyTypes
import ru.citeck.ecos.records2.RecordConstants
import java.time.Instant

@Indexes(
    Index(columns = [DbEntity.EXT_ID], unique = true),
    Index(columns = [DbEntity.REF_ID], unique = true),
    Index(columns = [DbEntity.TYPE]),
    Index(columns = [DbEntity.MODIFIED]),
    Index(columns = [DbEntity.CREATED]),
    Index(columns = [DbEntity.NAME], caseInsensitive = true)
)
@DbLegacyTypes(DbLegacyEntity0::class)
class DbEntity {

    companion object {
        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val EXT_ID = "__ext_id"
        const val UPD_VERSION = "__upd_version"
        const val MODIFIED = "__modified"
        const val MODIFIER = "__modifier"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
        const val DELETED = "__deleted"
        const val TYPE = "__type"
        const val WORKSPACE = "__workspace"
        const val STATUS = "__status"
        const val NAME = "__name"
        const val REF_ID = "__ref_id"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var refId: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var extId: String = ""

    @Constraints(NOT_NULL)
    var updVersion: Long = 0

    @Constraints(NOT_NULL)
    var modified: Instant = Instant.EPOCH

    @Constraints(NOT_NULL)
    var modifier: Long = -1

    @Constraints(NOT_NULL)
    var created: Instant = Instant.EPOCH

    @Constraints(NOT_NULL)
    var creator: Long = -1

    @Constraints(NOT_NULL)
    var deleted: Boolean = false

    @Constraints(NOT_NULL)
    var type: Long = -1

    var workspace: Long? = null

    @Constraints(NOT_NULL)
    var status: String = ""

    @Constraints(NOT_NULL)
    var name: MLText = MLText.EMPTY

    var attributes: MutableMap<String, Any?> = LinkedHashMap()

    var legacyType: String = ""

    fun copy(): DbEntity {
        val newEntity = DbEntity()
        newEntity.id = id
        newEntity.extId = extId
        newEntity.refId = refId
        newEntity.updVersion = updVersion
        newEntity.modified = modified
        newEntity.modifier = modifier
        newEntity.created = created
        newEntity.creator = creator
        newEntity.deleted = deleted
        newEntity.type = type
        newEntity.workspace = workspace
        newEntity.status = status
        newEntity.name = name
        newEntity.attributes = LinkedHashMap(attributes)
        return newEntity
    }

    fun equals(other: Any?, ignoredAtts: Set<String>): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as DbEntity
        val result = id == other.id &&
            refId == other.refId &&
            extId == other.extId &&
            updVersion == other.updVersion &&
            (ignoredAtts.contains(RecordConstants.ATT_MODIFIED) || modified == other.modified) &&
            (ignoredAtts.contains(RecordConstants.ATT_MODIFIER) || modifier == other.modifier) &&
            (ignoredAtts.contains(RecordConstants.ATT_CREATED) || created == other.created) &&
            (ignoredAtts.contains(RecordConstants.ATT_CREATOR) || creator == other.creator) &&
            deleted == other.deleted &&
            type == other.type &&
            workspace == other.workspace &&
            status == other.status &&
            name == other.name

        if (!result) {
            return false
        }
        if (ignoredAtts.isEmpty()) {
            return attributes == other.attributes
        }
        if (attributes.size == other.attributes.size) {
            for ((k, v) in attributes) {
                if (ignoredAtts.contains(k)) {
                    continue
                }
                if (v != other.attributes[k]) {
                    return false
                }
            }
            return true
        } else {
            val checkedKeys = HashSet<String>(ignoredAtts)
            for ((k, v) in attributes) {
                if (!checkedKeys.add(k)) {
                    continue
                }
                if (v != other.attributes[k]) {
                    return false
                }
            }
            for ((k, v) in other.attributes) {
                if (!checkedKeys.add(k)) {
                    continue
                }
                if (v != attributes[k]) {
                    return false
                }
            }
        }
        return true
    }

    override fun equals(other: Any?): Boolean {
        return equals(other, emptySet())
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + refId.hashCode()
        result = 31 * result + extId.hashCode()
        result = 31 * result + updVersion.hashCode()
        result = 31 * result + modified.hashCode()
        result = 31 * result + modifier.hashCode()
        result = 31 * result + created.hashCode()
        result = 31 * result + creator.hashCode()
        result = 31 * result + deleted.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + workspace.hashCode()
        result = 31 * result + status.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + attributes.hashCode()
        return result
    }
}
