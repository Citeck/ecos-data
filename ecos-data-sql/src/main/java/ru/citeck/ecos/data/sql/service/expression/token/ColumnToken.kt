package ru.citeck.ecos.data.sql.service.expression.token

class ColumnToken(val name: String) : ExpressionToken {

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
    }

    override fun toString(): String {
        return if (name.contains(".")) {
            name.split(".").joinToString(".") { "\"$it\"" }
        } else {
            "\"" + name + "\""
        }
    }
    override fun validate() {
        if (name.any { !it.isLetterOrDigit() && it != '-' && it != '_' && it != '.' && it != '\"' }) {
            throw IllegalArgumentException("Invalid column name '$name'")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as ColumnToken
        if (name != other.name) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}
