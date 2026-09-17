package ru.citeck.ecos.data.sql.columnmeta

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes
import java.time.Instant

/**
 * One row per physical column of an attribute, current or backed up. Schema-level: a single table
 * holds the rows of every domain table of the schema, which is why [TABLE_ID] is part of every
 * lookup.
 *
 * The flag column is `__backup`, not the `__is_backup` of the design note: a Kotlin property named
 * `isBackup` breaks [ru.citeck.ecos.data.sql.repo.entity.DbEntityMapperImpl], which looks the
 * backing field up by the JavaBeans property name and would search for a field called `backup`.
 */
@Indexes(
    Index(columns = [DbColumnMetaEntity.TABLE_ID, DbColumnMetaEntity.COLUMN_NAME], unique = true),
    Index(columns = [DbColumnMetaEntity.TABLE_ID, DbColumnMetaEntity.ATT_ID])
)
class DbColumnMetaEntity {

    companion object {
        const val TABLE = "ed_column_meta"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val TABLE_ID = "__table"
        const val COLUMN_NAME = "__column_name"
        const val ATT_ID = "__att_id"
        const val ATT_TYPE = "__att_type"
        const val MULTIPLE = "__multiple"
        const val BACKUP = "__backup"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(DbColumnConstraint.NOT_NULL)
    var table: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var columnName: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var attId: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var attType: String = ""

    @Constraints(DbColumnConstraint.NOT_NULL)
    var multiple: Boolean = false

    @Constraints(DbColumnConstraint.NOT_NULL)
    var backup: Boolean = false

    @Constraints(DbColumnConstraint.NOT_NULL)
    var created: Instant = Instant.EPOCH

    @Constraints(DbColumnConstraint.NOT_NULL)
    var creator: String = ""
}
