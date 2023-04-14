package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

@Indexes(
    Index(columns = [DbAssocEntity.SOURCE_ID, DbAssocEntity.ATTRIBUTE, DbAssocEntity.TARGET_ID], unique = true),
    Index(columns = [DbAssocEntity.TARGET_ID, DbAssocEntity.ATTRIBUTE])
)
class DbAssocEntity {

    companion object {
        const val TABLE = "ecos_associations"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val SOURCE_ID = "__source_id"
        const val TARGET_ID = "__target_id"
        const val ATTRIBUTE = "__attribute_id"
        const val CHILD = "__child"
        const val INDEX = "__index"
        const val DELETED = "__deleted"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
    }

    @Constraints(DbColumnConstraint.NOT_NULL)
    var sourceId: Long = -1

    @Constraints(DbColumnConstraint.NOT_NULL)
    var targetId: Long = -1

    @Constraints(DbColumnConstraint.NOT_NULL)
    var attributeId: Long = -1

    @Constraints(DbColumnConstraint.NOT_NULL)
    var child: Boolean = false

    @Constraints(DbColumnConstraint.NOT_NULL)
    var index: Int = 0

    @Constraints(DbColumnConstraint.NOT_NULL)
    var deleted: Boolean = false

    @Constraints(DbColumnConstraint.NOT_NULL)
    var created: Instant = Instant.EPOCH

    @Constraints(DbColumnConstraint.NOT_NULL)
    var creator: Long = -1
}
