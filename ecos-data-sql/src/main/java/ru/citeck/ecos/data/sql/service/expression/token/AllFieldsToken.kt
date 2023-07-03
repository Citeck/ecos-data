package ru.citeck.ecos.data.sql.service.expression.token

object AllFieldsToken : ExpressionToken {

    override fun validate() {}

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
    }

    override fun toString(): String {
        return "*"
    }

    override fun equals(other: Any?) = this === other

    override fun hashCode() = "*".hashCode()
}
