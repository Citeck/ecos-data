package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.commons.json.serialization.annotation.IncludeNonDefault
import ru.citeck.ecos.webapp.api.entity.EntityRef

@IncludeNonDefault
data class DbAssocChanges(
    val baseRef: EntityRef = EntityRef.EMPTY,
    val add: List<DbAssocChange> = emptyList(),
    val rem: List<DbAssocChange> = emptyList()
) {
    data class DbAssocChange(
        /**
         * Is target assoc or source
         */
        val target: Boolean = true,
        /**
         * Association attribute identifier
         */
        val assocId: String = "",
        /**
         * Reference to create or remove association
         */
        val ref: EntityRef
    )
}
