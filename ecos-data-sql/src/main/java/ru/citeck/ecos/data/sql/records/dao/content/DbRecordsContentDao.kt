package ru.citeck.ecos.data.sql.records.dao.content

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadInitReq
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadInitResp
import ru.citeck.ecos.data.sql.content.upload.ChunkedUploadPolicy
import ru.citeck.ecos.data.sql.content.upload.DbChunkedUploadService
import ru.citeck.ecos.data.sql.records.DbRecordsDao
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecordsAttsDao
import ru.citeck.ecos.data.sql.records.dao.atts.content.HasEcosContentDbData
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsContentDao : DbRecordsDaoCtxAware {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private lateinit var config: DbRecordsDaoConfig
    private lateinit var attsDao: DbRecordsAttsDao
    private lateinit var daoCtx: DbRecordsDaoCtx

    private var defaultContentStorage: EcosContentStorageConfig? = null

    override fun setRecordsDaoCtx(recordsDaoCtx: DbRecordsDaoCtx) {
        this.daoCtx = recordsDaoCtx
        config = daoCtx.config
        attsDao = daoCtx.attsDao
    }

    fun uploadFile(
        ecosType: String? = null,
        name: String? = null,
        mimeType: String? = null,
        encoding: String? = null,
        attributes: ObjectData? = null,
        writer: (EcosContentWriter) -> Unit
    ): EntityRef {
        return TxnContext.doInTxn {
            uploadFileInTxn(
                ecosType = ecosType,
                name = name,
                mimeType = mimeType,
                encoding = encoding,
                attributes = attributes,
                writer = writer
            )
        }
    }

    private fun uploadFileInTxn(
        ecosType: String?,
        name: String?,
        mimeType: String?,
        encoding: String?,
        attributes: ObjectData?,
        writer: (EcosContentWriter) -> Unit
    ): EntityRef {

        val typeId = (ecosType ?: "").ifBlank { config.typeRef.getLocalId() }
        if (typeId.isBlank()) {
            error("Type is blank. Uploading is impossible")
        }
        val typeInfo = daoCtx.ecosTypeService.getTypeInfoNotNull(typeId)
        val currentUserRefId = daoCtx.getOrCreateUserRefId(AuthContext.getCurrentUser())

        val contentId = daoCtx.recContentHandler.uploadContent(
            name,
            mimeType,
            encoding,
            getContentStorage(typeInfo),
            currentUserRefId,
            writer
        ) ?: error("File uploading failed")

        val result = createRecordForContent(typeInfo, name, attributes, contentId)
        // this content record already cloned while mutation and should be deleted
        daoCtx.contentService?.removeContent(contentId)
        return result
    }

    /**
     * Creates a record of type [typeInfo] whose content attribute points at the already-registered
     * content row [contentId]. Shared by [uploadFileInTxn] (streamed upload) and
     * [chunkedUploadComplete] (chunked upload finish).
     *
     * A workspace, when the caller wants one, travels in [attributes] as
     * [RecordConstants.ATT_WORKSPACE] like any other attribute of the record to be - both paths
     * carry it that way, and neither this method nor a chunked session has to model it separately.
     *
     * The record is always created in **this** DAO, through [DbRecordsDaoCtx.mutateDao]. Picking the
     * DAO by type is the caller's job and happens before a single byte is uploaded: for the
     * single-shot path `EcosContentServiceImpl` resolves the type's `sourceId` to an application and
     * a records DAO and calls `uploadFile` on it, and the chunked API expects its REST layer to do
     * the same at `init` and to keep the whole session on that DAO. [typeInfo] therefore only says
     * *which type of record* this DAO creates (a DAO can serve several), never *where* it is created
     * - see [checkTypeCanBeCreatedHere], which refuses a type served by another DAO.
     *
     * The caller is still responsible for removing [contentId] afterwards (the mutation below
     * clones it into the record's own content row - see [ru.citeck.ecos.data.sql.records.dao.content.DbRecContentHandler.uploadContent]).
     */
    private fun createRecordForContent(
        typeInfo: TypeInfo,
        name: String?,
        attributes: ObjectData?,
        contentId: Long
    ): EntityRef {

        val contentAttribute = resolveContentAttribute(typeInfo)

        val recordToMutate = LocalRecordAtts()
        recordToMutate.setAtt(contentAttribute, contentId)
        recordToMutate.setAtt("name", name)
        recordToMutate.setAtt(RecordConstants.ATT_TYPE, ModelUtils.getTypeRef(typeInfo.id))
        attributes?.forEach { key, value ->
            recordToMutate.setAtt(key, value)
        }
        return daoCtx.recContentHandler.withContentDbDataAware {
            EntityRef.create(daoCtx.appName, daoCtx.sourceId, daoCtx.mutateDao.mutate(recordToMutate))
        }
    }

    /**
     * The attribute a record of [typeInfo] holds its content in.
     *
     * Fails with an [IllegalArgumentException] rather than an [IllegalStateException] on a type that
     * cannot hold content at all: it is the caller's choice of type that is wrong, and the webmvc
     * content controller answers 400 with the exception message for that class. The messages below
     * name an attribute id only, which is safe to hand back to the client.
     */
    private fun resolveContentAttribute(typeInfo: TypeInfo): String {
        val contentAttribute = typeInfo.contentConfig.path.ifBlank { "content" }
        if (contentAttribute.contains(".")) {
            throw IllegalArgumentException("You can't upload file with content as complex path: '$contentAttribute'")
        }
        if (typeInfo.model.attributes.all { it.id != contentAttribute } &&
            typeInfo.model.systemAttributes.all { it.id != contentAttribute }
        ) {
            throw IllegalArgumentException("Content attribute is not found: $contentAttribute")
        }
        return contentAttribute
    }

    /**
     * Fails unless a record of [typeInfo] would be created by this DAO anyway - that is, unless the
     * type's source id names this very DAO.
     *
     * Picking the DAO by type is the caller's job, and it happens before a single byte is uploaded:
     * on the single-shot path `EcosContentServiceImpl` resolves the type's `sourceId` to an
     * application and a records DAO and calls `uploadFile` on that DAO. The chunked API expects the
     * same of its caller for the two calls that are about the record - `init` and `complete` -
     * because `complete` is what creates it: [createRecordForContent] mutates
     * [DbRecordsDaoCtx.mutateDao] directly. `writeChunk`, `getInfo` and `abort` in between are
     * schema-level: they only reach the session row, the content rows and the storage named by the
     * session, all of which belong to [DbSchemaContext], so any DAO of the same schema answers them
     * identically. The caller names the DAO it opened the session on in the upload id it hands its
     * own client, so the completion routes back to this DAO without a lookup.
     *
     * So this is a **check, not a resolution**, and it is a comparison of source ids. A type served
     * by another DAO is refused even when that DAO is in this very schema: the record would
     * otherwise land in this DAO's table under a type whose records are expected to live elsewhere,
     * which is exactly the mismatch the caller-side resolution exists to prevent.
     *
     * Comparing source ids subsumes everything a resolving version had to check one by one - a type
     * of another application, a type served by a
     * [RecordsDaoProxy][ru.citeck.ecos.records3.record.dao.impl.proxy.RecordsDaoProxy] or any other
     * non-[DbRecordsDao] source, a DAO in another schema: none of them has this DAO's own source id.
     *
     * Called at `init` so that the upload is refused before the client transfers gigabytes, and
     * again at `complete`, because the type's source id may be reconfigured while the session is in
     * flight and it is `complete` that actually writes the record.
     */
    private fun checkTypeCanBeCreatedHere(typeInfo: TypeInfo) {

        if (typeInfo.id.isNotBlank() && typeInfo.id == config.typeRef.getLocalId()) {
            return
        }

        val sourceId = typeInfo.sourceId
        if (sourceId.isBlank()) {
            error("Records dao can't be resolved for type '${typeInfo.id}': the type has no source id")
        }
        val localSrcId = toLocalSrcId(sourceId)
            ?: error(
                "Records of type '${typeInfo.id}' with source id '$sourceId' are served by " +
                    "another application, and only '${daoCtx.appName}' can be reached locally"
            )
        if (localSrcId != daoCtx.sourceId) {
            error(
                "Records of type '${typeInfo.id}' are served by source id '$sourceId', " +
                    "not by '${daoCtx.sourceId}' of application '${daoCtx.appName}', " +
                    "so this dao can't create them for an uploaded content"
            )
        }
    }

    /**
     * [sourceId] without this application's own app name prefix, or null when the prefix names
     * another application. A source id carrying no prefix at all is already local.
     */
    private fun toLocalSrcId(sourceId: String): String? {
        val delimIdx = sourceId.indexOf(EntityRef.APP_NAME_DELIMITER)
        if (delimIdx < 0) {
            return sourceId
        }
        if (sourceId.substring(0, delimIdx) != daoCtx.appName) {
            return null
        }
        return sourceId.substring(delimIdx + EntityRef.APP_NAME_DELIMITER.length)
    }

    @JvmOverloads
    fun getContent(recordId: String, attribute: String = "", index: Int = 0): EcosContentData? {

        val entity = daoCtx.attsDao.findDbEntityByExtId(recordId) ?: error("Entity doesn't found with id '$recordId'")

        val notBlankAttribute = if (attribute.isEmpty() || attribute == RecordConstants.ATT_CONTENT) {
            DbRecord.getDefaultContentAtt(daoCtx.getEntityMeta(entity).typeInfo)
        } else {
            attribute
        }

        val dotIdx = notBlankAttribute.indexOf('.')

        if (dotIdx > 0) {
            val contentApi = daoCtx.contentApi ?: error("ContentAPI is null")
            val pathBeforeDot = notBlankAttribute.substring(0, dotIdx)
            val pathAfterDot = notBlankAttribute.substring(dotIdx + 1)
            var linkedRefId = entity.attributes[pathBeforeDot]
            if (linkedRefId is Collection<*>) {
                linkedRefId = linkedRefId.firstOrNull()
            }
            return if (linkedRefId !is Long) {
                null
            } else {
                val entityRef = daoCtx.recordRefService.getEntityRefById(linkedRefId)
                contentApi.getContent(entityRef, pathAfterDot, index)
            }
        } else {
            val atts = attsDao.getRecordsAtts(listOf(recordId)).first()
            atts.init()
            val contentValue = atts.getAtt(notBlankAttribute)
            return if (contentValue is HasEcosContentDbData) {
                contentValue.getContentDbData()
            } else {
                null
            }
        }
    }

    fun getContentStorage(typeInfo: TypeInfo): EcosContentStorageConfig? {
        val storageRef = typeInfo.contentConfig.storageRef
        return if (storageRef.isEmpty() || storageRef == EcosContentStorageConstants.DEFAULT_CONTENT_STORAGE_REF) {
            defaultContentStorage
        } else {
            EcosContentStorageConfig(storageRef, typeInfo.contentConfig.storageConfig)
        }
    }

    fun setDefaultContentStorage(storage: EcosContentStorageConfig?) {
        this.defaultContentStorage = storage
    }

    // region chunked upload

    /**
     * Opens a chunked upload session for [req] and answers what the client needs to drive it.
     *
     * What this dao decides here is what only it can decide: that a record of [ChunkedUploadInitReq.ecosType]
     * can be created by it at all, and which content storage that type uploads to (its own, or this
     * dao's default). The session itself - the row, the digest, the storage-side upload - belongs to
     * the schema and is [DbChunkedUploadService]'s from this point on.
     *
     * Both type checks run before the storage-side upload is opened and before a session row exists,
     * so a refused init leaves nothing behind - and answers at init what would otherwise only be
     * discovered by [chunkedUploadComplete], after the whole file has been transferred.
     */
    fun chunkedUploadInit(req: ChunkedUploadInitReq, policy: ChunkedUploadPolicy): ChunkedUploadInitResp {

        // The type is what the record created at completion is made of, and there is no fallback
        // type: refuse the request instead of writing a session row that can never be completed.
        if (req.ecosType.isBlank()) {
            throw IllegalArgumentException("Ecos type is not specified for a chunked upload")
        }

        val typeInfo = daoCtx.ecosTypeService.getTypeInfoNotNull(req.ecosType)
        resolveContentAttribute(typeInfo)
        checkTypeCanBeCreatedHere(typeInfo)

        val storageCfg = getContentStorage(typeInfo)
        val storageRef = if (storageCfg == null || storageCfg.ref.isEmpty()) {
            EcosContentStorageConstants.LOCAL_CONTENT_STORAGE_REF
        } else {
            storageCfg.ref
        }

        return daoCtx.tableCtx.getSchemaCtx().chunkedUploadService.init(req, storageRef, policy)
    }

    /**
     * Finishes the upload and creates the record it was started for.
     *
     * Everything up to the record - assembling the object in the storage, typing it, registering the
     * content row - is [DbChunkedUploadService.complete]'s; this dao only says what record is made of
     * the result, inside the transaction the service opens for it.
     *
     * The type is re-checked here and not only at init: the type's source id may be reconfigured
     * while the session is in flight, and it is this call that writes the record. It is checked
     * inside the callback rather than before the whole completion, because the session - and
     * therefore the type it was opened for - is the service's to read; a type that moved away
     * therefore rolls back a content row that was just registered, which is the rare price of not
     * duplicating the session lookup.
     */
    fun chunkedUploadComplete(uploadId: String, policy: ChunkedUploadPolicy): EntityRef {
        return daoCtx.tableCtx.getSchemaCtx().chunkedUploadService.complete(uploadId, policy) { data ->
            val typeInfo = daoCtx.ecosTypeService.getTypeInfoNotNull(data.ecosType)
            checkTypeCanBeCreatedHere(typeInfo)
            createRecordForContent(typeInfo, data.name, data.attributes, data.contentId)
        }
    }

    // endregion
}
