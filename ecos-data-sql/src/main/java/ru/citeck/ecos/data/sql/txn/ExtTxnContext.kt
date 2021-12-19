package ru.citeck.ecos.data.sql.txn

import java.util.*

object ExtTxnContext {

    private val thExtTxnId = ThreadLocal<ExtTxnState>()
    private val thIsCommitting = ThreadLocal<Boolean>()

    fun <T> withExtTxn(txnId: UUID, readOnly: Boolean, action: () -> T): T {
        return withThreadVar(thExtTxnId, ExtTxnState(readOnly, txnId), action)
    }

    fun <T> withCommitting(action: () -> T): T {
        return withThreadVar(thIsCommitting, true, action)
    }

    private fun <T, V> withThreadVar(variable: ThreadLocal<V>, value: V, action: () -> T): T {
        val prevValue = variable.get()
        variable.set(value)
        try {
            return action.invoke()
        } finally {
            if (prevValue != null) {
                variable.set(prevValue)
            } else {
                variable.remove()
            }
        }
    }

    fun getExtTxnId(): UUID? {
        return thExtTxnId.get()?.txnId
    }

    fun isCommitting(): Boolean {
        return thIsCommitting.get() ?: false
    }

    data class ExtTxnState(val readOnly: Boolean, val txnId: UUID)
}
