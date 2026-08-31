package ru.citeck.ecos.data.sql.content

import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef

interface DbContentService {

    fun uploadContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storage: EcosContentStorageConfig?,
        creatorRefId: Long,
        content: (EcosContentWriter) -> Unit
    ): DbEcosContentData

    /**
     * Register content metadata that is already known (e.g. finished chunked upload)
     * without streaming any bytes through this service. The caller is responsible for
     * making sure [dataKey] actually points to existing data in [storageRef].
     *
     * [mimeType] is stored in the canonical form of [ru.citeck.ecos.commons.mime.MimeTypes.parse]
     * and a blank or unparseable value becomes `application/octet-stream`. No bytes are read here,
     * so a [mimeType] the caller left unspecific is never refined from the content itself.
     */
    fun registerContent(
        name: String?,
        mimeType: String?,
        encoding: String?,
        storageRef: EntityRef,
        dataKey: String,
        sha256: String,
        size: Long,
        creatorRefId: Long
    ): DbEcosContentData

    fun removeContent(id: Long)

    fun getContent(id: Long): DbEcosContentData?

    /**
     * Look up an already-registered content row by its (storageRef, dataKey) pair - used to make a
     * retried [registerContent] call idempotent (e.g. resuming a chunked upload's `complete` step
     * after a crash) without duplicating the row.
     */
    fun findContentByStorageAndDataKey(storageRef: EntityRef, dataKey: String): DbEcosContentData?

    fun cloneContent(id: Long, creatorRefId: Long): DbEcosContentData

    fun resetColumnsCache()
}
