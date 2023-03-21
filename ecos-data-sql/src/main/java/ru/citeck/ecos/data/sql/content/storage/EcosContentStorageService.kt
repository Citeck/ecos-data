package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.InputStream
import java.net.URI

interface EcosContentStorageService {

    fun uploadContent(storage: String, action: (EcosContentWriter) -> Unit): URI

    fun <T> readContent(uri: URI, action: (InputStream) -> T): T

    fun removeContent(uri: URI)

    fun register(storage: EcosContentStorage)

    fun register(type: String, storage: EcosContentStorage)
}
