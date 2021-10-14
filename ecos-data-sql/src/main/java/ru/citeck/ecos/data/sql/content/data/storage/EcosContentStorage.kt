package ru.citeck.ecos.data.sql.content.data.storage

import java.io.InputStream
import java.io.OutputStream

interface EcosContentStorage {

    fun writeContent(action: (OutputStream) -> Unit): EcosContentStorageMeta

    fun <T> readContent(path: String, action: (InputStream) -> T): T

    fun getType(): String
}
