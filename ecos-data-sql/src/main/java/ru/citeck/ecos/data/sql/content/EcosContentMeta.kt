package ru.citeck.ecos.data.sql.content

import java.time.Instant

data class EcosContentMeta(
    val id: Long,
    val name: String,
    val sha256: String,
    val size: Long,
    val mimeType: String,
    val encoding: String,
    val created: Instant
) {
    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): EcosContentMeta {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var id: Long = -1
        var name: String = ""
        var sha256: String = ""
        var size: Long = 0L
        var mimeType: String = ""
        var encoding: String = ""
        var created: Instant = Instant.EPOCH

        constructor(base: EcosContentMeta) : this() {
            this.id = base.id
            this.name = base.name
            this.size = base.size
            this.sha256 = base.sha256
            this.mimeType = base.mimeType
            this.encoding = base.encoding
            this.created = base.created
        }

        fun withId(id: Long): Builder {
            this.id = id
            return this
        }

        fun withName(name: String): Builder {
            this.name = name
            return this
        }

        fun withSize(size: Long): Builder {
            this.size = size
            return this
        }

        fun withSha256(sha256: String): Builder {
            this.sha256 = sha256
            return this
        }

        fun withMimeType(mimeType: String): Builder {
            this.mimeType = mimeType
            return this
        }

        fun withEncoding(encoding: String): Builder {
            this.encoding = encoding
            return this
        }

        fun withCreated(created: Instant): Builder {
            this.created = created
            return this
        }

        fun build(): EcosContentMeta {
            return EcosContentMeta(
                id,
                name,
                sha256,
                size,
                mimeType,
                encoding,
                created
            )
        }
    }
}
