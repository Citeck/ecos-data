package ru.citeck.ecos.data.sql.utils

import java.lang.Exception

/* Kotlin extensions from jdk7 lib */

fun <T : AutoCloseable, R> T.use(action: (T) -> R): R {
    var exception: Throwable? = null
    try {
        return action.invoke(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        try {
            this.closeFinally(exception)
        } catch (e: Exception) {
            // do nothing
        }
    }
}

fun AutoCloseable?.closeFinally(cause: Throwable?) = when {
    this == null -> {}
    cause == null -> close()
    else -> {
        try {
            close()
        } catch (closeException: Throwable) {
            cause.addSuppressed(closeException)
        }
    }
}
