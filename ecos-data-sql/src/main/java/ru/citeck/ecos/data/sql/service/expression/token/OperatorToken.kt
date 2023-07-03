package ru.citeck.ecos.data.sql.service.expression.token

class OperatorToken(val type: Type) : ExpressionToken {

    companion object {

        private val SINGLE_CHAR_OPERATORS = setOf(
            '+',
            '-',
            '/',
            '%',
            '*',
            '='
        )
        private val OPERATOR_CHARS = setOf(
            *SINGLE_CHAR_OPERATORS.toTypedArray(),
            '<',
            '>'
        )

        fun isOperatorChar(char: Char): Boolean {
            return OPERATOR_CHARS.contains(char)
        }

        fun valueOf(value: String): OperatorToken {
            return OperatorToken(Type.values().first { it.value.equals(value, true) })
        }

        fun getIdxAfterOperator(value: String, fromIdx: Int): Int {
            val ch = value[fromIdx]
            if (ch !in OPERATOR_CHARS &&
                !ch.equals('a', true) &&
                !ch.equals('o', true)
            ) {
                return fromIdx
            }
            if (SINGLE_CHAR_OPERATORS.contains(value[fromIdx])) {
                return fromIdx + 1
            }
            val char0 = value[fromIdx]
            val char1 = if (value.length > fromIdx + 1) { value[fromIdx + 1] } else { null }
            return if (char0 == '<') {
                if (char1 == '=' || char1 == '>') {
                    fromIdx + 2
                } else {
                    fromIdx + 1
                }
            } else if (char0 == '>') {
                if (char1 == '=') {
                    fromIdx + 2
                } else {
                    fromIdx + 1
                }
            } else {
                val char2 = if (value.length > fromIdx + 2) { value[fromIdx + 2] } else { null }
                val char3 = if (value.length > fromIdx + 3) { value[fromIdx + 3] } else { null }
                if (char0.equals('a', true) &&
                    char1?.equals('n', true) == true &&
                    char2?.equals('d', true) == true &&
                    char3?.isWhitespace() == true
                ) {
                    return fromIdx + 3
                } else if (
                    char0.equals('o', true) &&
                    char1?.equals('r', true) == true &&
                    char2?.isWhitespace() == true
                ) {
                    return fromIdx + 2
                } else {
                    fromIdx
                }
            }
        }
    }

    enum class Type(val value: String) {
        PLUS("+"),
        MINUS("-"),
        DIV("/"),
        REM("%"),
        MULTIPLY("*"),
        GREATER(">"),
        LESS("<"),
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS_OR_EQUAL("<="),
        GREATER_OR_EQUAL(">="),
        AND("AND"),
        OR("OR")
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
    }

    override fun toString(): String {
        return type.value
    }

    override fun validate() {}

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as OperatorToken
        if (type != other.type) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return type.hashCode()
    }
}
