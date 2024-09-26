package ru.citeck.ecos.data.sql.content.entity

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

@Indexes(
    Index(columns = [DbContentEntity.SHA_256, DbContentEntity.SIZE]),
    Index(columns = [DbContentEntity.STORAGE_REF, DbContentEntity.DATA_KEY])
)
class DbContentEntity {

    companion object {

        const val TABLE = "ed_content"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val SHA_256 = "__sha256"
        const val NAME = "__name"
        const val SIZE = "__size"
        const val MIMETYPE = "__mime_type"
        const val ENCODING = "__encoding"
        const val DATA_KEY = "__data_key"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
        const val STORAGE_REF = "__storage_ref"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var size: Long = 0

    @Constraints(NOT_NULL)
    var name: String = ""

    @Constraints(NOT_NULL)
    var mimeType: String = ""

    @Constraints(NOT_NULL)
    var encoding: String = ""

    @Constraints(NOT_NULL)
    var sha256: String = ""

    @Constraints(NOT_NULL)
    var dataKey: String = ""

    var storageRef: Long? = -1

    var created: Instant = Instant.EPOCH

    var creator: Long = -1
}
