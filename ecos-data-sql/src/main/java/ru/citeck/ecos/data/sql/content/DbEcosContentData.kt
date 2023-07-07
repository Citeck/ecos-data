package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.data.sql.content.storage.EcosContentDataUrl
import ru.citeck.ecos.webapp.api.content.EcosContentData

interface DbEcosContentData : EcosContentData {

    fun getDbId(): Long

    fun getUrl(): EcosContentDataUrl
}
