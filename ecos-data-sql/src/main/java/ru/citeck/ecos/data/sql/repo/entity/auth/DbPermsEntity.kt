package ru.citeck.ecos.data.sql.repo.entity.auth

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbPermsEntity.RECORD_ID, DbPermsEntity.AUTHORITY_ID], unique = true),
    Index(columns = [DbPermsEntity.AUTHORITY_ID, DbPermsEntity.RECORD_ID])
)
class DbPermsEntity(
    @Constraints(NOT_NULL)
    var recordId: Long,
    @Constraints(NOT_NULL)
    var authorityId: Long
) {
    companion object {
        const val RECORD_ID = "__record_id"
        const val AUTHORITY_ID = "__authority_id"
    }

    constructor() : this(-1, -1)
}
