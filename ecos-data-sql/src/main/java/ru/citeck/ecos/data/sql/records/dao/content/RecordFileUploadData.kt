package ru.citeck.ecos.data.sql.records.dao.content

import ru.citeck.ecos.commons.data.ObjectData
import java.io.InputStream

data class RecordFileUploadData(
    val ecosType: String,
    val name: String,
    val mimeType: String,
    val encoding: String,
    val attributes: ObjectData,
    val content: InputStream
) {

    companion object {
        @JvmStatic
        fun create(): Builder {
            return Builder()
        }
    }

    class Builder {

        var ecosType: String = ""
        var name: String = ""
        var mimeType: String = ""
        var encoding: String = ""
        var attributes: ObjectData = ObjectData.create()
        lateinit var content: InputStream

        fun withEcosType(ecosType: String?): Builder {
            this.ecosType = ecosType ?: ""
            return this
        }

        fun withName(name: String?): Builder {
            this.name = name ?: ""
            return this
        }

        fun withMimeType(mimeType: String?): Builder {
            this.mimeType = mimeType ?: ""
            return this
        }

        fun withEncoding(encoding: String?): Builder {
            this.encoding = encoding ?: ""
            return this
        }

        fun withContent(content: InputStream): Builder {
            this.content = content
            return this
        }

        fun withAttributes(attributes: ObjectData?): Builder {
            this.attributes = ObjectData.deepCopyOrNew(attributes)
            return this
        }

        fun build(): RecordFileUploadData {
            return RecordFileUploadData(
                ecosType = ecosType,
                name = name,
                mimeType = mimeType,
                encoding = encoding,
                attributes = attributes,
                content = content
            )
        }
    }
}
