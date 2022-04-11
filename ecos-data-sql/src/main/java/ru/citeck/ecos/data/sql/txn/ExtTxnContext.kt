package ru.citeck.ecos.data.sql.txn

import java.util.*

object ExtTxnContext {

    private val thExtTxnId = ThreadLocal<ExtTxnState>()
    private val thWithoutModifiedMeta = ThreadLocal<Boolean>()

    fun <T> withoutExtTxn(action: () -> T): T {
        return withThreadVar(thExtTxnId, null, action)
    }

    fun <T> withExtTxn(txnId: UUID, readOnly: Boolean, action: () -> T): T {
        return withThreadVar(thExtTxnId, ExtTxnState(readOnly, txnId), action)
    }

    fun <T> withoutModifiedMeta(action: () -> T): T {
        return withThreadVar(thWithoutModifiedMeta, true, action)
    }

    private fun <T, V> withThreadVar(variable: ThreadLocal<V>, value: V?, action: () -> T): T {
        val prevValue = variable.get()
        if (value == null) {
            variable.remove()
        } else {
            variable.set(value)
        }
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

    fun isWithoutModifiedMeta(): Boolean {
        return thWithoutModifiedMeta.get() ?: false
    }

    data class ExtTxnState(val readOnly: Boolean, val txnId: UUID)
}
