package ru.citeck.ecos.data.sql.pg

import ru.citeck.ecos.commons.data.DataValue
import java.util.*

object ContentUtils {

    fun createContentObjFromText(text: String, fileName: String = "", fileType: String = ""): DataValue {

        val contentMimeType = "text/plain"
        val contentBytes = text.toByteArray(Charsets.UTF_8)
        val resultName = fileName.ifBlank { UUID.randomUUID().toString() + ".txt" }

        val contentValue = DataValue.create(
            """
              {
                "storage": "base64",
                "name": "$resultName-${UUID.randomUUID()}.txt",
                "url": "data:$contentMimeType;base64,${Base64.getEncoder().encodeToString(contentBytes)}",
                "size": ${contentBytes.size},
                "type": "$contentMimeType",
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
