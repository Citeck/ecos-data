package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.webapp.api.content.EcosContentWriter

interface DbContentService {

    fun uploadContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storage: EcosContentStorageConfig?,
        creatorRefId: Long,
        content: (EcosContentWriter) -> Unit
    ): DbEcosContentData

    fun removeContent(id: Long)

    fun getContent(id: Long): DbEcosContentData?

    fun cloneContent(id: Long, creatorRefId: Long): DbEcosContentData

    fun resetColumnsCache()
}
