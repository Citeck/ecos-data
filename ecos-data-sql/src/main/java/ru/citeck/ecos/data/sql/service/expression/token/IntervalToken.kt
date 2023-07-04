package ru.citeck.ecos.data.sql.service.expression.token

class IntervalToken(val value: String) : ExpressionToken {

    companion object {
        private val VALID_INTERVAL_CHARS = setOf(
            '-',
            '+',
            ' ',
            *(('a'..'z').toList().toTypedArray()),
            *(('A'..'Z').toList().toTypedArray()),
            *(('0'..'1').toList().toTypedArray())
        )
    }

    override fun validate() {
        if (!value.all { VALID_INTERVAL_CHARS.contains(it) }) {
            error("invalid interval: $value")
        }
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
    }

    override fun toString(): String {
        return "INTERVAL '$value'"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as IntervalToken
        if (value != other.value) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return value.hashCode()
    }
}
