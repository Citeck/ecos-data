package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

@Indexes(
    Index(columns = [DbEntity.EXT_ID], unique = true),
    Index(columns = [DbEntity.DELETED]),
    Index(columns = [DbEntity.MODIFIED]),
    Index(columns = [DbEntity.CREATED])
)
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
        const val STATUS = "__status"
        const val NAME = "__name"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var extId: String = ""

    @Constraints(NOT_NULL)
    var updVersion: Long = 0

    @Constraints(NOT_NULL)
    var modified: Instant = Instant.EPOCH
    @Constraints(NOT_NULL)
    var modifier: String = ""

    @Constraints(NOT_NULL)
    var created: Instant = Instant.EPOCH
    @Constraints(NOT_NULL)
    var creator: String = ""

    @Constraints(NOT_NULL)
    var deleted: Boolean = false

    @Constraints(NOT_NULL)
    var type: String = ""
    @Constraints(NOT_NULL)
    var status: String = ""
    @Constraints(NOT_NULL)
    var name: MLText = MLText.EMPTY

    var attributes: MutableMap<String, Any?> = LinkedHashMap()
}
