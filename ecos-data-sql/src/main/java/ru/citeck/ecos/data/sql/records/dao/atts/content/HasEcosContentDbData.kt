package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.data.sql.content.EcosContentDbData

interface HasEcosContentDbData {

    fun getContentDbData(): EcosContentDbData
}
