package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.data.sql.content.DbEcosContentData
import ru.citeck.ecos.records3.record.atts.value.impl.AttValueDelegate
import ru.citeck.ecos.webapp.api.mime.MimeType
import java.io.InputStream
import java.time.Instant

class DbContentValueWithCustomName(
    private val name: String,
    private val value: DbContentValue
) : AttValueDelegate(value), HasEcosContentDbData {

    fun getContentValue(): DbContentValue {
        return value
    }

    override fun getDisplayName(): Any {
        return getNameWithExt()
    }

    override fun getContentDbData(): DbEcosContentData {
        return DbEcosContentDataWithName(value.contentData)
    }

    override fun getAs(type: String): Any? {
        val result = super.getAs(type)
        if (result is DbContentValue.ContentData) {
            return result.copy(name = getNameWithExt())
        }
        return result
    }

    override fun getAtt(name: String): Any? {
        if (name == "name") {
            return getNameWithExt()
        }
        return super.getAtt(name)
    }

    private fun getNameWithExt(): String {
        val extension = value.getAtt(DbContentValue.ATT_EXTENSION) as? String ?: ""
        if (extension.isNotEmpty()) {
            val extensionWithDot = ".$extension"
            if (!name.endsWith(extensionWithDot)) {
                return name + extensionWithDot
            }
        }
        return name
    }

    private inner class DbEcosContentDataWithName(
        private val value: DbEcosContentData
    ) : DbEcosContentData {
        override fun getCreated(): Instant = value.getCreated()
        override fun getCreator(): String = value.getCreator()
        override fun getEncoding(): String = value.getEncoding()
        override fun getMimeType(): MimeType = value.getMimeType()
        override fun getName(): String = getNameWithExt()
        override fun getSha256(): String = value.getSha256()
        override fun getSize(): Long = value.getSize()
        override fun <T> readContent(action: (InputStream) -> T): T {
            return value.readContent(action)
        }
        override fun getStorageRef() = value.getStorageRef()
        override fun getDataKey(): String = value.getDataKey()
        override fun getDbId(): Long = value.getDbId()
    }
}
