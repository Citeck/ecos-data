package ru.citeck.ecos.data.sql.batch

import java.util.concurrent.ConcurrentHashMap

/**
 * Open registry of handlers, one per data source.
 *
 * Registration is not overwritable. A handler type is written into `ed_batch_task.__handler` and
 * outlives the process that queued it, so silently swapping the implementation behind a type is
 * how a queued task ends up executed by code that was never meant to see it.
 */
class DbBatchTaskHandlerRegistry {

    private val handlers = ConcurrentHashMap<String, DbBatchTaskHandler>()

    fun register(handler: DbBatchTaskHandler) {
        val previous = handlers.putIfAbsent(handler.getType(), handler)
        if (previous != null && previous !== handler) {
            error(
                "Batch task handler with type '${handler.getType()}' is already registered: " +
                    "${previous::class.java.name}. Types identify rows that outlive this process, " +
                    "so they can't be reassigned at runtime."
            )
        }
    }

    fun getHandler(type: String): DbBatchTaskHandler? {
        return handlers[type]
    }

    fun getTypes(): Set<String> {
        return handlers.keys.toSet()
    }
}
