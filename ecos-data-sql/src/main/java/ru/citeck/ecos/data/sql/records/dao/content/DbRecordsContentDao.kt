package ru.citeck.ecos.data.sql.records.dao.content

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConfig
import ru.citeck.ecos.data.sql.content.storage.EcosContentStorageConstants
import ru.citeck.ecos.data.sql.records.DbRecordsDaoConfig
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtxAware
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecord
import ru.citeck.ecos.data.sql.records.dao.atts.DbRecordsAttsDao
import ru.citeck.ecos.data.sql.records.dao.atts.content.HasEcosContentDbData
import ru.citeck.ecos.data.sql.records.dao.mutate.DbRecordsMutateDao
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.LocalRecordAtts
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.content.EcosContentData
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsContentDao : DbRecordsDaoCtxAware {

    private lateinit var config: DbRecordsDaoConfig
    private lateinit var attsDao: DbRecordsAttsDao
    private lateinit var mutateDao: DbRecordsMutateDao
    private lateinit var daoCtx: DbRecordsDaoCtx

    private var defaultContentStorage: EcosContentStorageConfig? = null

    override fun setRecordsDaoCtx(recordsDaoCtx: DbRecordsDaoCtx) {
        this.daoCtx = recordsDaoCtx
        config = daoCtx.config
        attsDao = daoCtx.attsDao
        mutateDao = daoCtx.mutateDao
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

        val contentAttribute = typeInfo.contentConfig.path.ifBlank { "content" }
        if (contentAttribute.contains(".")) {
            error("You can't upload file with content as complex path: '$contentAttribute'")
        }
        if (typeInfo.model.attributes.all { it.id != contentAttribute } &&
            typeInfo.model.systemAttributes.all { it.id != contentAttribute }
        ) {
            error("Content attribute is not found: $contentAttribute")
        }
        val currentUserRefId = daoCtx.getOrCreateUserRefId(AuthContext.getCurrentUser())

        val contentId = daoCtx.recContentHandler.uploadContent(
            name,
            mimeType,
            encoding,
            getContentStorage(typeInfo),
            currentUserRefId,
            writer
        ) ?: error("File uploading failed")

        val recordToMutate = LocalRecordAtts()
        recordToMutate.setAtt(contentAttribute, contentId)
        recordToMutate.setAtt("name", name)
        recordToMutate.setAtt(RecordConstants.ATT_TYPE, ModelUtils.getTypeRef(typeId))
        attributes?.forEach { key, value ->
            recordToMutate.setAtt(key, value)
        }
        val result = daoCtx.recContentHandler.withContentDbDataAware {
            EntityRef.create(daoCtx.appName, daoCtx.sourceId, mutateDao.mutate(recordToMutate))
        }
        // this content record already cloned while mutation and should be deleted
        daoCtx.contentService?.removeContent(contentId)
        return result
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
}
