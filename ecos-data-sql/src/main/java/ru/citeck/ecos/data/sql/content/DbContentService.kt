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

    fun removeContent(id: Long)

    fun getContent(id: Long): DbEcosContentData?

    fun cloneContent(id: Long): DbEcosContentData
}
