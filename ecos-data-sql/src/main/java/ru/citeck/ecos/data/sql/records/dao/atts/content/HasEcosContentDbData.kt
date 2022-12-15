package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.data.sql.content.DbEcosContentData

interface HasEcosContentDbData {

    fun getContentDbData(): DbEcosContentData
}
