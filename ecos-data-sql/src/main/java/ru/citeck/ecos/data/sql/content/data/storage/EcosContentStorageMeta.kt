package ru.citeck.ecos.data.sql.content.data.storage

data class EcosContentStorageMeta(
    val path: String,
    val sha256: String,
    val size: Long
)
