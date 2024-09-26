package ru.citeck.ecos.data.sql.records.attnames

import ru.citeck.ecos.data.sql.dto.DbColumnConstraint
import ru.citeck.ecos.data.sql.repo.entity.annotation.Constraints
import ru.citeck.ecos.data.sql.repo.entity.annotation.Index
import ru.citeck.ecos.data.sql.repo.entity.annotation.Indexes

@Indexes(
    Index(columns = [DbEcosAttributeEntity.EXT_ID])
)
class DbEcosAttributeEntity {

    companion object {
        const val TABLE = "ed_attributes"

        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val EXT_ID = "__ext_id"
    }

    @Constraints(DbColumnConstraint.PRIMARY_KEY)
    var id: Long = NEW_REC_ID

    @Constraints(DbColumnConstraint.NOT_NULL)
    var extId: String = ""
}
