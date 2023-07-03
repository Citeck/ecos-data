package ru.citeck.ecos.data.sql.service.expression.token

class NullConditionToken(val isNull: Boolean) : ExpressionToken {

    override fun validate() {
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
    }

    override fun toString(): String {
        return if (isNull) {
            "IS NULL"
        } else {
            "IS NOT NULL"
        }
    }
}
