package ru.citeck.ecos.data.sql.columnmeta

import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

/**
 * How a model attribute type is stored physically.
 *
 * Extracted from [ru.citeck.ecos.data.sql.ecostype.DbEcosModelService] so that the migration
 * machinery can ask the same question without going through a model lookup: "would a column of this
 * physical shape be what this attribute type produces?" The mapping is many-to-one - 15 attribute
 * types onto 8 column types - which is the whole reason the column registry exists.
 */
object DbAttTypeColumns {

    @JvmStatic
    fun getColumnType(attType: AttributeType): DbColumnType {
        return when (attType) {
            AttributeType.ASSOC -> DbColumnType.LONG
            AttributeType.PERSON -> DbColumnType.LONG
            AttributeType.AUTHORITY_GROUP -> DbColumnType.LONG
            AttributeType.AUTHORITY -> DbColumnType.LONG
            AttributeType.ENTITY_REF -> DbColumnType.LONG
            AttributeType.OPTIONS -> DbColumnType.TEXT
            AttributeType.TEXT -> DbColumnType.TEXT
            AttributeType.MLTEXT -> DbColumnType.TEXT
            AttributeType.NUMBER -> DbColumnType.DOUBLE
            AttributeType.BOOLEAN -> DbColumnType.BOOLEAN
            AttributeType.DATE -> DbColumnType.DATE
            AttributeType.DATETIME -> DbColumnType.DATETIME
            AttributeType.CONTENT -> DbColumnType.LONG
            AttributeType.JSON -> DbColumnType.JSON
            AttributeType.BINARY -> DbColumnType.BINARY
        }
    }

    /**
     * CONTENT is a single `ed_content` id whatever the attribute definition says, so it is never
     * stored as an array.
     */
    @JvmStatic
    fun isMultiple(attType: AttributeType, multiple: Boolean): Boolean {
        return attType != AttributeType.CONTENT && multiple
    }
}
