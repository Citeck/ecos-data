package ru.citeck.ecos.data.sql.content.data

import java.io.InputStream
import java.io.OutputStream
import java.net.URI

interface EcosContentDataService {

    fun writeContent(storage: String, action: (OutputStream) -> Unit): EcosContentDataMeta

    fun <T> readContent(uri: URI, action: (InputStream) -> T): T
}
