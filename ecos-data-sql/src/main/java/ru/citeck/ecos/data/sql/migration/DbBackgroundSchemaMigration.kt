package ru.citeck.ecos.data.sql.migration

import ru.citeck.ecos.data.sql.dto.DbTableRef

/** The background reconciler must yield instead of waiting behind a record transaction. */
internal object DbBackgroundSchemaMigration {
    private val enabled = ThreadLocal.withInitial { false }

    val active: Boolean get() = enabled.get()

    fun <T> run(action: () -> T): T {
        val previous = enabled.get()
        enabled.set(true)
        return try {
            action()
        } finally {
            if (previous) enabled.set(true) else enabled.remove()
        }
    }
}

/** Expected contention: rollback the background attempt and retry it on a later tick. */
class DbSchemaMigrationBusyException(table: DbTableRef, cause: Throwable) : RuntimeException("Background schema migration deferred for busy table ${table.fullName}", cause)
