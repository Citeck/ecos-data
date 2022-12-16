package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.webapp.api.content.EcosContentWriter

interface DbContentService {

    fun init()

    fun uploadContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storage: String?,
        writer: (EcosContentWriter) -> Unit
    ): DbEcosContentData

    fun getContent(id: Long): DbEcosContentData?
}
