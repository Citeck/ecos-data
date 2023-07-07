package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.webapp.api.constants.AppName
import ru.citeck.ecos.webapp.api.entity.EntityRef

object EcosContentStorageConstants {

    const val CONTENT_STORAGE_SRC_ID = "content-storage"

    val DEFAULT_CONTENT_STORAGE_REF = EntityRef.create(
        "emodel",
        CONTENT_STORAGE_SRC_ID,
        "DEFAULT"
    )

    val LOCAL_CONTENT_STORAGE_REF = EntityRef.create(
        AppName.EMODEL,
        CONTENT_STORAGE_SRC_ID,
        "LOCAL"
    )
}
