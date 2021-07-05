package ru.citeck.ecos.data.sql.auth

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.Constraints
import ru.citeck.ecos.data.sql.repo.entity.DbEntity

class DbAuthorityEntity {

    @Constraints(PRIMARY_KEY)
    var id: Long = DbEntity.NEW_REC_ID

    @Constraints(NOT_NULL)
    var extId: String = ""
}
