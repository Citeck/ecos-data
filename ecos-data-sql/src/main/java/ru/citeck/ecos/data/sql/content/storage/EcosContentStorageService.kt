package ru.citeck.ecos.data.sql.content.storage

import java.io.InputStream
import java.io.OutputStream

interface EcosContentStorageService {

    fun uploadContent(storageConfig: EcosContentStorageConfig?, action: (OutputStream) -> Unit): EcosContentDataUrl

    fun <T> readContent(url: EcosContentDataUrl, action: (InputStream) -> T): T

    fun deleteContent(url: EcosContentDataUrl)
}
