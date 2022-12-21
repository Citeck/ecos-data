package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.webapp.api.content.EcosContentData
import java.net.URI

interface DbEcosContentData : EcosContentData {

    fun getDbId(): Long

    fun getUri(): URI
}
