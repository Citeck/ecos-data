package ru.citeck.ecos.data.sql.service.expression.token

class ScalarToken(val value: Any) : ExpressionToken {

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
    }

    override fun toString(): String {
        if (value is String) {
            return "'$value'"
        }
        return value.toString()
    }

    override fun validate() {
        if (value is String) {
            if (value.contains('\'')) {
                error("Invalid string scalar: $value")
            }
            return
        }
        if (value is Boolean || value is Long || value is Double) {
            return
        }
        error("Invalid value type: ${value::class}")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as ScalarToken
        if (value != other.value) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return value.hashCode()
    }
}
