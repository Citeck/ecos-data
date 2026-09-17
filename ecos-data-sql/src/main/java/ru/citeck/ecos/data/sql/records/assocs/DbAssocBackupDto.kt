package ru.citeck.ecos.data.sql.records.assocs

import java.time.Instant

/**
 * Immutable view of a [DbAssocBackupEntity] row.
 *
 * Wider than [DbAssocDto] on purpose: [index], [created] and [creator] have to survive
 * the departure from the association group, and they are exactly the fields [DbAssocDto] does not
 * carry. Restoring a link with today's timestamp and today's author would be a different link.
 */
data class DbAssocBackupDto(
    val sourceId: Long,
    val targetId: Long,
    val attributeId: Long,
    val child: Boolean,
    val index: Int,
    val created: Instant,
    val creator: Long
)
