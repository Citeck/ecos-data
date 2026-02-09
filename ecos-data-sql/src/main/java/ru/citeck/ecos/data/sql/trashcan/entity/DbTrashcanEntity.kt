package ru.citeck.ecos.data.sql.trashcan.entity

import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

@Indexes(
    Index(columns = [DbTrashcanEntity.REF_ID]),
    Index(columns = [DbTrashcanEntity.DELETED_AT]),
    Index(columns = [DbTrashcanEntity.NAME], caseInsensitive = true)
)
class DbTrashcanEntity {

    companion object {

        const val TABLE = "ed_trashcan"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val REF_ID = "__ref_id"
        const val SOURCE_TABLE = "__source_table"
        const val NAME = "__name"
        const val DELETED_AT = "__deleted_at"
        const val DELETED_BY = "__deleted_by"
        const val DELETED_AS = "__deleted_as"
        const val TYPE = "__type"
        const val TRACE_ID = "__trace_id"
        const val TXN_ID = "__txn_id"
        const val ENTITY_DATA = "__entity_data"
        const val CONTENT_IDS = "__content_ids"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var refId: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var sourceTable: String = ""

    @Constraints(NOT_NULL)
    var name: MLText = MLText.EMPTY

    @Constraints(NOT_NULL)
    var type: Long = -1

    @Constraints(NOT_NULL)
    var traceId: String = ""

    @Constraints(NOT_NULL)
    var txnId: String = ""

    @Constraints(NOT_NULL)
    var deletedAt: Instant = Instant.EPOCH

    @Constraints(NOT_NULL)
    var deletedBy: Long = -1

    @Constraints(NOT_NULL)
    var deletedAs: Long = -1

    @Constraints(NOT_NULL)
    @ColumnType(DbColumnType.JSON)
    var entityData: String = "{}"

    @ColumnType(DbColumnType.LONG)
    var contentIds: List<Long> = emptyList()
}
