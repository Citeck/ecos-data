package ru.citeck.ecos.data.sql.service.expression.token

class BracesToken(val tokens: List<ExpressionToken>) : ExpressionToken {

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
                if (token is OperatorToken || token is NullConditionToken) {
                    error("Invalid tokens: $tokens")
                }
            } else {
                if (token !is OperatorToken && token !is NullConditionToken) {
                    error("Invalid tokens: $tokens")
                }
            }
            if (token is AllFieldsToken) {
                error("All fields token '*' can be used only within count function")
            }
            operandExpected = !operandExpected
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as BracesToken
        if (tokens != other.tokens) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return tokens.hashCode()
    }
}
