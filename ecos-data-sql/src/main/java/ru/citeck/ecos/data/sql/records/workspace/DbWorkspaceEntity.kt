package ru.citeck.ecos.data.sql.records.workspace

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.records.refs.DbRecordRefEntity
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbRecordRefEntity.EXT_ID], unique = true)
)
class DbWorkspaceEntity {

    companion object {
        const val TABLE = "ecos_workspace"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val EXT_ID = "__ext_id"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = DbRecordRefEntity.NEW_REC_ID

    @Constraints(DbColumnConstraint.NOT_NULL)
    var extId: String = ""

    override fun toString(): String {
        return "{\"id\":$id,\"extId\":\"$extId\"}"
    }
}
