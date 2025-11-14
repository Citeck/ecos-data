package ru.citeck.ecos.data.sql.service.expression.token

import ru.citeck.ecos.data.sql.dto.DbColumnType

class FunctionToken(val name: String, val args: List<ExpressionToken>) : ExpressionToken {

    companion object {
        val AGG_FUNCTIONS = setOf(
            "avg",
            "count",
            "max",
            "min",
            "sum"
        )
        val NUM_FUNCTIONS = setOf(
            "abs", "ceil", "ceiling", "div", "exp", "floor",
            "mod", "power", "round",
            "sign", "sqrt", "trunc", "pi"
        )
        val RANDOM_FUNCTIONS = setOf(
            "random"
        )
        val DATETIME_FUNCTIONS = setOf(
            "age", "current_date", "current_time", "current_timestamp",
            "clock_timestamp", "date_bin", "date_part", "date_trunc",
            "isfinite", "justify_days", "justify_hours", "justify_interval",
            "make_date", "make_interval", "make_time", "make_timestamp",
            "make_timestamptz", "statement_timestamp", "timeofday",
            "transaction_timestamp", "extract", "localtime",
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
            "substring", "translate", "trim", "upper", "substringBefore", "concat", "concat_ws"
        )
        val OTHER_FUNCTIONS = setOf(
            "coalesce",
            "nullif",
            "greatest",
            "least"
        )
        val CUSTOM_FUNCTIONS = setOf(
            "startOfMonth",
            "endOfMonth"
        )
        private val ALLOWED_FUNCTIONS = setOf(
            *AGG_FUNCTIONS.toTypedArray(),
            *RANDOM_FUNCTIONS.toTypedArray(),
            *DATETIME_FUNCTIONS.toTypedArray(),
            *NUM_FUNCTIONS.toTypedArray(),
            *CONVERSION_FUNCTIONS.toTypedArray(),
            *STRING_FUNCTIONS.toTypedArray(),
            *OTHER_FUNCTIONS.toTypedArray(),
            *CUSTOM_FUNCTIONS.toTypedArray()
        )
        val FUNCTIONS_WITHOUT_BRACES = setOf(
            "current_date"
        )

        fun getFunctionReturnType(token: ExpressionToken): DbColumnType {
            if (token !is FunctionToken) {
                return DbColumnType.DOUBLE
            }
            val name = token.name
            return if (name == "to_char" || STRING_FUNCTIONS.contains(name)) {
                DbColumnType.TEXT
            } else if (NUM_FUNCTIONS.contains(name) || AGG_FUNCTIONS.contains(name)) {
                DbColumnType.DOUBLE
            } else if (name == "to_timestamp" || DATETIME_FUNCTIONS.contains(name)) {
                DbColumnType.DATETIME
            } else if (name == "to_date") {
                DbColumnType.DATE
            } else if (name == "coalesce" && token.args.isNotEmpty()) {
                getFunctionReturnType(token.args.first())
            } else {
                DbColumnType.DOUBLE
            }
        }
    }

    fun isAggregationFunc(): Boolean {
        return AGG_FUNCTIONS.contains(name)
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        if (type.isInstance(this)) {
            visitor.invoke(type.cast(this))
        }
        args.forEach { it.visit(type, visitor) }
    }

    override fun toString(): String {
        return toString { it.toString() }
    }

    override fun toString(converter: (ExpressionToken) -> String): String {
        return if (FUNCTIONS_WITHOUT_BRACES.contains(name)) {
            name
        } else {
            when (name) {
                "substring" -> {
                    var str = "substring(" + converter(args[0]) + " from " + converter(args[1])
                    if (args.size > 2) {
                        str += " for " + converter(args[2])
                    }
                    str += ")"
                    str
                }
                "substringBefore" -> {
                    "substring(${args[0].toString(converter)}, 0, " +
                        "position(${args[1].toString(converter)} IN ${args[0].toString(converter)}))"
                }
                else -> {
                    "$name(${args.joinToString { it.toString(converter) }})"
                }
            }
        }
    }

    override fun validate() {
        if (!ALLOWED_FUNCTIONS.contains(name)) {
            error("Unknown function: '$name'")
        }
        if (args.any { it is AllFieldsToken } && name != "count") {
            error("All fields token '*' can be used only within count function")
        }
        args.forEach { it.validate() }
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
