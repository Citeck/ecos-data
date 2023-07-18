package ru.citeck.ecos.data.sql.content.storage

import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.io.InputStream
import java.io.OutputStream

interface EcosContentStorage {

    fun uploadContent(storageRef: EntityRef, storageConfig: ObjectData, content: (OutputStream) -> Unit): String

    fun <T> readContent(storageRef: EntityRef, dataKey: String, action: (InputStream) -> T): T

    fun deleteContent(storageRef: EntityRef, dataKey: String)
}
