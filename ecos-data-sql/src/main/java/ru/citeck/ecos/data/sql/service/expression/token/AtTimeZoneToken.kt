package ru.citeck.ecos.data.sql.service.expression.token

class AtTimeZoneToken(val value: String) : ExpressionToken {

    override fun validate() {
        if (value.contains('\'')) {
            error("Invalid timezone: '$value'")
        }
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AtTimeZoneToken

        if (value != other.value) return false

        return true
    }

    override fun hashCode(): Int {
        return value.hashCode()
    }

    override fun toString(): String {
        return "AT TIME ZONE '$value'"
    }
}
