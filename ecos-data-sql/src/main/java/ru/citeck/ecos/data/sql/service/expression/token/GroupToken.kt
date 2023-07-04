package ru.citeck.ecos.data.sql.service.expression.token

class GroupToken(val tokens: List<ExpressionToken>) : ExpressionToken {

    companion object {
        fun wrapIfRequired(tokens: List<ExpressionToken>): ExpressionToken {
            return if (tokens.size == 1) {
                tokens.first()
            } else {
                GroupToken(tokens)
            }
        }
    }

    constructor(vararg tokens: ExpressionToken) : this(tokens.toList())

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
        tokens.forEach { it.visit(type, visitor) }
    }

    override fun toString(): String {
        return "(" + tokens.joinToString(" ") + ")"
    }

    override fun validate() {
        if (tokens.isEmpty()) {
            error("Tokens is empty")
        }
        var operandExpected = true
        for (token in tokens) {
            if (operandExpected) {
                if (token is OperatorToken || token is NullConditionToken || token is AtTimeZoneToken) {
                    error("Invalid tokens: $tokens")
                }
            } else {
                if (token !is OperatorToken && token !is NullConditionToken && token !is AtTimeZoneToken) {
                    error("Invalid tokens: $tokens")
                }
            }
            if (token is AllFieldsToken) {
                error("All fields token '*' can be used only within count function")
            }
            if (token !is NullConditionToken && token !is AtTimeZoneToken) {
                operandExpected = !operandExpected
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as GroupToken
        if (tokens != other.tokens) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return tokens.hashCode()
    }
}
