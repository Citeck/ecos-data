package ru.citeck.ecos.data.sql.meta.schema

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbEntity.EXT_ID], unique = true)
)
class DbSchemaMetaEntity {

    companion object {
        const val TABLE = "ed_schema_meta"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val EXT_ID = "__ext_id"
        const val VALUE = "__value"
    }

    @Constraints(PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(NOT_NULL)
    var updVersion: Long = 0

    @Constraints(NOT_NULL)
    var extId: String = ""

    @Constraints(NOT_NULL)
    @ColumnType(DbColumnType.JSON)
    var value: String = "null"
}
