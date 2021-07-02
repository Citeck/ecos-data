package ru.citeck.ecos.data.sql.meta

import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.repo.entity.Constraints
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.repo.entity.DbFieldType
import java.time.Instant

class DbTableMetaEntity {

    @Constraints("PRIMARY KEY")
    var id: Long = DbEntity.NEW_REC_ID

    @Constraints("NOT NULL")
    var extId: String = ""

    @Constraints("NOT NULL")
    var updVersion: Long = 0

    @Constraints("NOT NULL")
    var modified: Instant = Instant.EPOCH
    @Constraints("NOT NULL")
    var modifier: String = ""

    @Constraints("NOT NULL")
    var created: Instant = Instant.EPOCH
    @Constraints("NOT NULL")
    var creator: String = ""

    @DbFieldType(DbColumnType.JSON)
    var config: String = "{}"

    @DbFieldType(DbColumnType.JSON)
    var changelog: String = "[]"
}
