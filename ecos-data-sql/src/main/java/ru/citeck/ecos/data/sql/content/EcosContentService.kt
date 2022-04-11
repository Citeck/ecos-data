package ru.citeck.ecos.data.sql.content

import java.io.InputStream
import java.io.OutputStream

interface EcosContentService {

    fun writeContent(storage: String, meta: EcosContentMeta, bytes: ByteArray): EcosContentMeta

    fun writeContent(storage: String, meta: EcosContentMeta, action: (OutputStream) -> Unit): EcosContentMeta

    fun getMeta(id: Long): EcosContentMeta?

    fun <T> readContent(id: Long, action: (EcosContentMeta, InputStream) -> T): T
}
