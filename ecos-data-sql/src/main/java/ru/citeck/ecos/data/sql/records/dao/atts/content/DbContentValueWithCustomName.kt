package ru.citeck.ecos.data.sql.records.dao.atts.content

import ru.citeck.ecos.data.sql.content.EcosContentDbData
import ru.citeck.ecos.records3.record.atts.value.impl.AttValueDelegate
import java.io.InputStream
import java.time.Instant

class DbContentValueWithCustomName(
    private val name: String,
    private val value: DbContentValue
) : AttValueDelegate(value), HasEcosContentDbData {

    override fun getContentDbData(): EcosContentDbData {
        return EcosContentDbDataWithName(name, value.contentData)
    }

    override fun getAs(type: String): Any? {
        val result = super.getAs(type)
        if (result is DbContentValue.ContentData) {
            return result.copy(name = this.name)
        }
        return result
    }

    override fun getAtt(name: String): Any? {
        if (name == "name") {
            return this.name
        }
        return super.getAtt(name)
    }

    private class EcosContentDbDataWithName(
        private val name: String,
        private val value: EcosContentDbData
    ) : EcosContentDbData {
        override fun getCreated(): Instant = value.getCreated()
        override fun getEncoding(): String = value.getEncoding()
        override fun getMimeType(): String = value.getMimeType()
        override fun getName(): String = name
        override fun getSha256(): String = value.getSha256()
        override fun getSize(): Long = value.getSize()
        override fun <T> readContent(action: (InputStream) -> T): T {
            return value.readContent(action)
        }
        override fun getDbId(): Long = value.getDbId()
    }
}
