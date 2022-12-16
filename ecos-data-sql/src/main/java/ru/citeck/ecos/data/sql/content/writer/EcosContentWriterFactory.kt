package ru.citeck.ecos.data.sql.content.writer

import ru.citeck.ecos.webapp.api.content.EcosContentWriter
import java.io.OutputStream

interface EcosContentWriterFactory {

    fun createWriter(output: OutputStream): EcosContentWriter
}
