package ru.citeck.ecos.data.sql.content.upload

/**
 * Lifecycle of a chunked upload session: ACTIVE -> COMPLETING -> DONE, or `*` -> ABORTED.
 * DONE and ABORTED are terminal.
 *
 * Persisted as its own name in [DbContentUploadSessionEntity.status], which is a plain string
 * column - the entity mapper has no enum support.
 */
enum class ContentUploadSessionStatus {

    ACTIVE,
    COMPLETING,
    DONE,
    ABORTED;

    val isTerminal: Boolean
        get() = this == DONE || this == ABORTED

    companion object {

        /**
         * `null` for a value no version of this code ever wrote, i.e. a corrupt row.
         */
        @JvmStatic
        fun ofOrNull(status: String): ContentUploadSessionStatus? {
            return entries.firstOrNull { it.name == status }
        }

        @JvmStatic
        fun of(status: String): ContentUploadSessionStatus {
            return ofOrNull(status) ?: error("Unknown upload session status: '$status'")
        }
    }
}
