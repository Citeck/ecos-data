package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.webapp.api.entity.EntityRef

data class EcosContentStorageConfig(val ref: EntityRef, val config: ObjectData)
