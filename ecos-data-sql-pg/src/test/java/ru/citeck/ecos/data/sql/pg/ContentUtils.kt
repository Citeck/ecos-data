package ru.citeck.ecos.data.sql.pg

import ru.citeck.ecos.commons.data.DataValue
import java.util.*

object ContentUtils {

    fun createContentObjFromText(
        text: String,
        fileName: String = "",
        fileType: String = "",
        mimeType: String = "text/plain"
    ): DataValue {
        return createContentObj(text.toByteArray(Charsets.UTF_8), fileName, fileType, mimeType)
    }

    fun createContentObj(
        data: ByteArray,
        fileName: String = "",
        fileType: String = "",
        mimeType: String = "text/plain"
    ): DataValue {

        val resultName = fileName.ifBlank { UUID.randomUUID().toString() + ".txt" }

        val contentValue = DataValue.create(
            """
              {
                "storage": "base64",
                "name": "$resultName-${UUID.randomUUID()}.txt",
                "url": "data:$mimeType;base64,${Base64.getEncoder().encodeToString(data)}",
                "size": ${data.size},
                "type": "$mimeType",
                "originalName": "$resultName"
              }
            """
        )

        if (fileType.isNotEmpty()) {
            contentValue["fileType"] = fileType
        }

        return contentValue
    }
}
