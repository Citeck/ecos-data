package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.webapp.api.content.EcosContentData

interface EcosContentDbData : EcosContentData {

    fun getDbId(): Long
}
