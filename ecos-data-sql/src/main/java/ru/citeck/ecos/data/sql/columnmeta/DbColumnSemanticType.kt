package ru.citeck.ecos.data.sql.columnmeta

import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

/**
 * What a physical column actually holds, as far as the column registry knows.
 *
 * This cannot be derived from [DbColumnType]: 15 [AttributeType] values map onto 8 [DbColumnType]
 * values, so an attribute going ASSOC -> ENTITY_REF, or TEXT -> MLTEXT, leaves the physical schema
 * untouched while changing what the stored bytes mean. The registry therefore stores the semantic
 * type explicitly.
 *
 * [Raw] exists for columns the registry meets for the first time on an installation that predates
 * it and whose physical type contradicts the model - the state a failed conversion leaves behind.
 * There is no [AttributeType] to record there, only the fact that the column holds something of a
 * given physical shape, so the migration machinery treats it as an unknown source type.
 */
sealed interface DbColumnSemanticType {

    companion object {

        private const val RAW_PREFIX = "raw_"

        private val modelByName = AttributeType.entries.associateBy { it.name.lowercase() }
        private val rawByName = DbColumnType.entries.associateBy { it.name.lowercase() }

        /**
         * Parses the stored form back. Throws on anything unrecognised: downgrading the platform is
         * forbidden in this product, so a row written by a version this instance does not
         * understand is not a version-skew case to degrade gracefully from - it is a corrupted
         * column registry row or a bug, and continuing on a silently misread registry value is
         * exactly how a column ends up migrated in the wrong direction.
         */
        @JvmStatic
        fun parse(value: String): DbColumnSemanticType {
            if (value.isNotEmpty()) {
                if (value.startsWith(RAW_PREFIX)) {
                    rawByName[value.substring(RAW_PREFIX.length)]?.let { return Raw(it) }
                } else {
                    modelByName[value]?.let { return Model(it) }
                }
            }
            error("Unrecognised column registry semantic type: '$value'")
        }
    }

    /**
     * The form stored in `ed_column_meta.__att_type`.
     */
    fun asString(): String

    /**
     * The column's type is known from the type model.
     */
    data class Model(val attType: AttributeType) : DbColumnSemanticType {
        override fun asString(): String = attType.name.lowercase()
    }

    /**
     * The column's model type was never recorded and its physical type contradicts the model, so
     * only the physical shape is known. The [RAW_PREFIX] keeps this form disjoint from every
     * [Model] form: no [AttributeType] is named `raw_...`.
     */
    data class Raw(val columnType: DbColumnType) : DbColumnSemanticType {
        override fun asString(): String = RAW_PREFIX + columnType.name.lowercase()
    }
}
