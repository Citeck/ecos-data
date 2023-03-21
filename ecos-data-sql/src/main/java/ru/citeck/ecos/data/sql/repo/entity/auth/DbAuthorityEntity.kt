package ru.citeck.ecos.data.sql.repo.entity.auth

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbEntity.EXT_ID], unique = true)
)
class DbAuthorityEntity {

    companion object {
        const val TABLE = "ecos_authorities"

        const val ID = "id"
        const val EXT_ID = "__ext_id"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = DbEntity.NEW_REC_ID

    @Constraints(NOT_NULL)
    var extId: String = ""
}
