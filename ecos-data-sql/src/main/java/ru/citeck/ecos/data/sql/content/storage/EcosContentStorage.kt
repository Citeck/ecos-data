package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.data.sql.content.writer.EcosContentWriterFactory
import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.InputStream

interface EcosContentStorage {

    fun init(writerFactory: EcosContentWriterFactory)

    /**
     * Upload content to storage and get path to it.
     * Returned path should be readable by readContent(path, action) method
     * This method may return same path for equal content.
     * Implementation may process argument "type" or ignore it.
     *
     * @param type storage subtype. It is a second part of content storage type.
     *             e.g. if type config -> content config -> storage type equal to "remote/abc", then
     *             this argument will be "abc" and getType() of EcosContentStorage implementation will be "remote"
     *
     * @return path to content delimited by "/". This path should define full path to content to allow
     *         read method works without any additional arguments.
     */
    fun uploadContent(type: String, writer: (EcosContentWriter) -> Unit): String

    fun <T> readContent(path: String, action: (InputStream) -> T): T

    fun removeContent(path: String)

    fun getType(): String
}
