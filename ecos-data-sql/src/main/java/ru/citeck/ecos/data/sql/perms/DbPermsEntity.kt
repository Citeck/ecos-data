package ru.citeck.ecos.data.sql.perms

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbPermsEntity.ENTITY_REF_ID, DbPermsEntity.AUTHORITY_ID], unique = true),
    Index(columns = [DbPermsEntity.AUTHORITY_ID, DbPermsEntity.ENTITY_REF_ID])
)
class DbPermsEntity(
    @Constraints(NOT_NULL)
    var entityRefId: Long,
    @Constraints(NOT_NULL)
    var authorityId: Long,
    @Constraints(NOT_NULL)
    var allowed: Boolean
) {
    companion object {
        const val TABLE = "ecos_read_perms"

        const val ENTITY_REF_ID = "__entity_ref_id"
        const val AUTHORITY_ID = "__authority_id"
        const val ALLOWED = "__allowed"
    }

    constructor() : this(-1, -1, true)
}
