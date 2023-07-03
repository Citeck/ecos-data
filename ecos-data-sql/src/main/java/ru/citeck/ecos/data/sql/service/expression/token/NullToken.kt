package ru.citeck.ecos.data.sql.service.expression.token

object NullToken : ExpressionToken {

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
    }

    override fun toString(): String {
        return "null"
    }

    override fun validate() {}

    override fun equals(other: Any?) = this === other

    override fun hashCode() = "null".hashCode()
}
