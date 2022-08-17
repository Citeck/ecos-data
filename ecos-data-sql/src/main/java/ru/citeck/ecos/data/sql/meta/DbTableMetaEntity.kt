package ru.citeck.ecos.data.sql.meta

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint.*
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.annotation.ColumnType
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import java.time.Instant

class DbTableMetaEntity {

    @Constraints(PRIMARY_KEY)
    var id: Long = DbEntity.NEW_REC_ID

    @Constraints(NOT_NULL)
    var extId: String = ""

    @Constraints(NOT_NULL)
    var updVersion: Long = 0

    @Constraints(NOT_NULL)
    var modified: Instant = Instant.EPOCH

    @Constraints(NOT_NULL)
    var modifier: String = ""

    @Constraints(NOT_NULL)
    var created: Instant = Instant.EPOCH

    @Constraints(NOT_NULL)
    var creator: String = ""

    @ColumnType(DbColumnType.JSON)
    var config: String = "{}"

    @ColumnType(DbColumnType.JSON)
    var changelog: String = "[]"
}
