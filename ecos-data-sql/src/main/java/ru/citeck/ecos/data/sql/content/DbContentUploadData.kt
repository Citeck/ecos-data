package ru.citeck.ecos.data.sql.content

import java.io.InputStream

open class DbContentUploadData(
    val encoding: String,
    val mimeType: String,
    val name: String,
    val content: InputStream
) {

    companion object {

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }
    }

    class Builder {

        var encoding: String = ""
        var mimeType: String = ""
        var name: String = ""
        lateinit var content: InputStream

        fun withEncoding(encoding: String): Builder {
            this.encoding = encoding
            return this
        }

        fun withMimeType(mimeType: String): Builder {
            this.mimeType = mimeType
            return this
        }

        fun withName(name: String): Builder {
            this.name = name
            return this
        }

        fun withContent(content: InputStream): Builder {
            this.content = content
            return this
        }

        fun build(): DbContentUploadData {
            return DbContentUploadData(
                encoding = encoding,
                mimeType = mimeType,
                name = name,
                content = content
            )
        }
    }
}
