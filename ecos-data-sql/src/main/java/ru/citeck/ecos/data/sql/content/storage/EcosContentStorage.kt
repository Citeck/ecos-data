package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.InputStream

interface EcosContentStorage {

    fun init(writerFactory: EcosContentWriterFactory)

    fun uploadContent(type: String, writer: (EcosContentWriter) -> Unit): String

    fun <T> readContent(path: String, action: (InputStream) -> T): T

    fun removeContent(path: String)

    fun getType(): String
}
