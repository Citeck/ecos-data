package ru.citeck.ecos.data.sql.records.assocs

import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbAssocRefsDiff(
    val assocId: String,
    val added: List<EntityRef>,
    val removed: List<EntityRef>,
    val child: Boolean
)
