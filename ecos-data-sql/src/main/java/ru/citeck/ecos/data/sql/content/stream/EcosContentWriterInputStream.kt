package ru.citeck.ecos.data.sql.content.stream

import java.io.InputStream

abstract class EcosContentWriterInputStream : InputStream() {

    abstract fun getSha256Digest(): String

    abstract fun getContentSize(): Long
}
