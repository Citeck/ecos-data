package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbEcosContentData : EcosContentData {

    fun getDbId(): Long

    fun getPath(): String

    fun getStorageRef(): EntityRef
}
