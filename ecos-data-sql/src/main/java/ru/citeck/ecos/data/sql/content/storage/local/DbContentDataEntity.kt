package ru.citeck.ecos.data.sql.content.storage.local

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints

class DbContentDataEntity {

    companion object {

        const val TABLE = "ecos_content_data"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val DATA = "__data"

        private val EMPTY_DATA = ByteArray(0)
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var data: ByteArray = EMPTY_DATA
}
