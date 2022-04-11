package ru.citeck.ecos.data.sql.service.job.txn

import java.util.concurrent.TimeUnit

data class TxnDataCleanerConfig(
    val periodMs: Long,
    val initDelayMs: Long,
    val txnRecordLifeTimeMs: Long
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): TxnDataCleanerConfig {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    fun copy(): Builder {
        return Builder(this)
    }

    class Builder() {

        var periodMs: Long = TimeUnit.MINUTES.toMillis(5)
        var initDelayMs: Long = TimeUnit.SECONDS.toMillis(30)
        var txnRecordLifeTimeMs: Long = TimeUnit.MINUTES.toMillis(30)

        constructor(base: TxnDataCleanerConfig) : this() {
            this.periodMs = base.periodMs
            this.initDelayMs = base.initDelayMs
            this.txnRecordLifeTimeMs = base.txnRecordLifeTimeMs
        }

        fun withPeriodMs(periodMs: Long): Builder {
            this.periodMs = periodMs
            return this
        }

        fun withInitDelayMs(initDelayMs: Long): Builder {
            this.initDelayMs = initDelayMs
            return this
        }

        fun withTxnRecordLifeTimeMs(txnRecordLifeTimeMs: Long): Builder {
            this.txnRecordLifeTimeMs = txnRecordLifeTimeMs
            return this
        }

        fun build(): TxnDataCleanerConfig {
            return TxnDataCleanerConfig(
                periodMs,
                initDelayMs,
                txnRecordLifeTimeMs
            )
        }
    }
}
