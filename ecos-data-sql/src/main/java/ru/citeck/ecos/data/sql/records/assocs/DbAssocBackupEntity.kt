package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

/**
 * One row per association taken out of [DbAssocEntity.MAIN_TABLE] when its attribute
 * stopped being assoc-like.
 *
 * **Not `ed_associations_deleted`.** That table is a trash can: it exists to be cleaned up one day,
 * which would destroy a backup that is supposed to be permanent, and it already holds links the user
 * deleted on purpose. Restoring from a mixed table would resurrect those - not a loss of
 * distinguishability but a resurrection of deleted data.
 *
 * Keyed by the `ed_column_meta` id of the **backup column**, not by `(table, attId)`: one attribute
 * can be moved aside several times, and each departure needs its own snapshot of the links that
 * belonged to it at that moment.
 *
 * [created] and [creator] carry the association's **original** values, not the moment of the backup.
 */
@Indexes(
    Index(columns = [DbAssocBackupEntity.COLUMN_META_ID, DbAssocBackupEntity.SOURCE_ID]),
    Index(
        columns = [
            DbAssocBackupEntity.COLUMN_META_ID,
            DbAssocBackupEntity.SOURCE_ID,
            DbAssocBackupEntity.TARGET_ID
        ],
        unique = true
    )
)
class DbAssocBackupEntity {

    companion object {
        const val TABLE = "ed_associations_backup"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val COLUMN_META_ID = "__column_meta_id"
        const val SOURCE_ID = "__source_id"
        const val TARGET_ID = "__target_id"
        const val ATTRIBUTE = "__attribute_id"
        const val CHILD = "__child"
        const val INDEX = "__index"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(DbColumnConstraint.NOT_NULL)
    var columnMetaId: Long = -1

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
    var created: Instant = Instant.EPOCH

    @Constraints(DbColumnConstraint.NOT_NULL)
    var creator: Long = -1
}
