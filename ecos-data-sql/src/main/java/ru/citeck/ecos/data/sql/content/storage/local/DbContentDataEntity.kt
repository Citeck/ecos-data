package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbContentDataEntity.SHA_256, DbContentDataEntity.SIZE], unique = true)
)
class DbContentDataEntity {

    companion object {

        const val TABLE = "ecos_content_data"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val SHA_256 = "__sha256"
        const val DATA = "__data"
        const val SIZE = "__size"

        private val EMPTY_DATA = ByteArray(0)
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var data: ByteArray = EMPTY_DATA

    @Constraints(NOT_NULL)
    var size: Long = 0

    @Constraints(NOT_NULL)
    var sha256: String = ""
}
