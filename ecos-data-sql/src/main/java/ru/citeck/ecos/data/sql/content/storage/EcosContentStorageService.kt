package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.data.sql.content.stream.EcosContentWriterInputStream
import java.io.InputStream
import java.net.URI

interface EcosContentStorageService {

    fun uploadContent(storage: String, content: EcosContentWriterInputStream): URI

    fun getContent(uri: URI): InputStream
}
