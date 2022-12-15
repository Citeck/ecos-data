package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.data.sql.content.stream.EcosContentWriterInputStream
import java.io.InputStream

interface EcosContentStorage {

    fun uploadContent(type: String, content: EcosContentWriterInputStream): String

    fun <T> readContent(path: String, action: (InputStream) -> T): T

    fun removeContent(path: String)

    fun getType(): String
}
