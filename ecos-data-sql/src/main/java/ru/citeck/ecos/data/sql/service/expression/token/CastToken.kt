package ru.citeck.ecos.data.sql.service.expression.token

class CastToken(val token: ExpressionToken, val castTo: String) : ExpressionToken {

    companion object {
        const val DELIMITER = "::"
    }

    override fun validate() {
        token.validate()
        if (castTo.any { it !in 'a'..'z' && it !in 'A'..'Z' }) {
            error("Invalid castTo: '$castTo'")
        }
    }

    override fun toString(converter: (ExpressionToken) -> String): String {
        return "${token.toString(converter)}$DELIMITER$castTo"
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as CastToken
        if (token != other.token) {
            return false
        }
        if (castTo != other.castTo) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = token.hashCode()
        result = 31 * result + castTo.hashCode()
        return result
    }

    override fun toString(): String {
        return toString { it.toString() }
    }
}
