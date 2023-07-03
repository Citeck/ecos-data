package ru.citeck.ecos.data.sql.service.expression.token

class FunctionToken(val name: String, val args: List<ExpressionToken>) : ExpressionToken {

    companion object {
        val NUM_FUNCTIONS = setOf(
            "abs", "avg", "ceil", "ceiling", "count", "div", "exp", "floor",
            "max", "min", "mod", "power", "random", "round",
            "sign", "sqrt", "sum", "trunc"
        )
        val DATETIME_FUNCTIONS = setOf(
            "age", "current_date", "current_time", "current_timestamp",
            "clock_timestamp", "date_bin", "date_part", "date_trunc",
            "isfinite", "justify_days", "justify_hours", "justify_interval",
            "make_date", "make_interval", "make_time", "make_timestamp",
            "make_timestamptz", "statement_timestamp", "timeofday",
            "transaction_timestamp", "to_timestamp", "extract", "localtime",
            "localtimestamp", "now"
        )
        val CONVERSION_FUNCTIONS = setOf(
            "to_char",
            "to_date",
            "to_number",
            "to_timestamp"
        )
        val STRING_FUNCTIONS = setOf(
            "btrim", "char_length", "character_length", "initcap", "length", "lower",
            "lpad", "ltrim", "position", "repeat", "replace", "rpad", "rtrim", "strpos",
            "substring", "translate", "trim", "upper",
        )
        val OTHER_FUNCTIONS = setOf(
            "coalesce"
        )
        private val ALLOWED_FUNCTIONS = setOf(
            *DATETIME_FUNCTIONS.toTypedArray(),
            *NUM_FUNCTIONS.toTypedArray(),
            *CONVERSION_FUNCTIONS.toTypedArray(),
            *STRING_FUNCTIONS.toTypedArray(),
            *OTHER_FUNCTIONS.toTypedArray()
        )
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
        args.forEach { it.visit(type, visitor) }
    }

    override fun toString(): String {
        return "$name(${args.joinToString()})"
    }

    override fun validate() {
        if (!ALLOWED_FUNCTIONS.contains(name)) {
            error("Unknown function: '$name'")
        }
        if (args.any { it is AllFieldsToken } && name != "count") {
            error("All fields token '*' can be used only within count function")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as FunctionToken
        if (name != other.name) {
            return false
        }
        if (args != other.args) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + args.hashCode()
        return result
    }
}
