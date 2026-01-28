package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

@Indexes(
    Index(columns = [DbRecordRefMoveHistoryEntity.REF_ID]),
    Index(columns = [DbRecordRefMoveHistoryEntity.MOVED_AT])
)
class DbRecordRefMoveHistoryEntity {

    companion object {
        const val TABLE = "ed_record_ref_move_history"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val REF_ID = "__ref_id"
        const val MOVED_FROM = "__moved_from"
        const val MOVED_TO = "__moved_to"
        const val MOVED_AT = "__moved_at"
        const val MOVED_BY = "__moved_by"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    var refId: Long = -1
    var movedFrom: String = ""
    var movedTo: String = ""
    var movedBy: Long = -1L
    var movedAt: Instant = Instant.EPOCH

    override fun toString(): String {
        return "{" +
            "\"id\":$id," +
            "\"refId\":$refId," +
            "\"movedAt\":\"$movedAt\"," +
            "\"movedFrom\":\"$movedFrom\"," +
            "\"movedTo\":\"$movedTo\"," +
            "\"movedBy\":$movedBy" +
            "}"
    }
}
