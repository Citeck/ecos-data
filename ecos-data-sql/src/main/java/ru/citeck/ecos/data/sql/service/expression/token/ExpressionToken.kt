package ru.citeck.ecos.data.sql.service.expression.token

interface ExpressionToken {

    fun validate()

    fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit)

    fun visitColumns(visitor: (ColumnToken) -> Unit) {
        visit(ColumnToken::class.java, visitor)
    }

    fun visitFunctions(visitor: (FunctionToken) -> Unit) {
        visit(FunctionToken::class.java, visitor)
    }

    fun toString(converter: (ExpressionToken) -> String): String {
        return converter(this)
    }
}
