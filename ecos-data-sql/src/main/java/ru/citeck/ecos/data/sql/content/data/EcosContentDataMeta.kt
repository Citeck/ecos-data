package ru.citeck.ecos.data.sql.content.data

import java.net.URI

data class EcosContentDataMeta(
    val uri: URI,
    val sha256: String,
    val size: Long
)
