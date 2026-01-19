package ru.citeck.ecos.data.sql.records.refs

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

@Indexes(
    Index(columns = [DbRecordRefMigrationEntity.FROM_REF]),
    Index(columns = [DbRecordRefMigrationEntity.TIME])
)
class DbRecordRefMigrationEntity {

    companion object {
        const val TABLE = "ed_record_ref_migration"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val FROM_REF = "__from_ref"
        const val TO_REF = "__to_ref"
        const val TIME = "__time"
        const val MIGRATED_BY = "__migrated_by"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    var time: Instant = Instant.EPOCH
    var fromRef: Long = -1L
    var toRef: Long = -1L
    var migratedBy: Long = -1L

    override fun toString(): String {
        return "{" +
            "\"id\":$id," +
            "\"time\":\"$time\"," +
            "\"fromRef\":$fromRef," +
            "\"toRef\":$toRef," +
            "\"migratedBy\":$migratedBy" +
            "}"
    }
}
