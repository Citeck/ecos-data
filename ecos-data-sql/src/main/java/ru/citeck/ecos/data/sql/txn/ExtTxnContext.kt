package ru.citeck.ecos.data.sql.txn

import java.util.*

object ExtTxnContext {

    private val thExtTxnId = ThreadLocal<ExtTxnState>()

    fun <T> withExtTxn(txnId: UUID, readOnly: Boolean, action: () -> T): T {
        val currentState = thExtTxnId.get()
        thExtTxnId.set(ExtTxnState(readOnly, txnId))
        try {
            return action.invoke()
        } finally {
            if (currentState != null) {
                thExtTxnId.set(currentState)
            } else {
                thExtTxnId.remove()
            }
        }
    }

    fun getExtTxnId(): UUID? {
        return thExtTxnId.get()?.txnId
    }

    data class ExtTxnState(val readOnly: Boolean, val txnId: UUID)
}
