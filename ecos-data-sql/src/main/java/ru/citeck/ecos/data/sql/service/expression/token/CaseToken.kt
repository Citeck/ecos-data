package ru.citeck.ecos.data.sql.service.expression.token

import java.lang.StringBuilder

class CaseToken(
    val branches: List<Branch>,
    val orElse: ExpressionToken?,
) : ExpressionToken {

    override fun validate() {
        branches.forEach {
            it.condition.validate()
            it.thenResult.validate()
        }
        orElse?.validate()
    }

    override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        branches.forEach {
            it.condition.visit(type, visitor)
            it.thenResult.visit(type, visitor)
        }
        orElse?.visit(type, visitor)
    }

    override fun toString(converter: (ExpressionToken) -> String): String {
        val result = StringBuilder("CASE")
        branches.forEach {
            result.append(" WHEN ${it.condition.toString(converter)} THEN ${it.thenResult.toString(converter)}")
        }
        if (orElse != null) {
            result.append(" ELSE ${orElse.toString(converter)}")
        }
        result.append(" END")
        return result.toString()
    }

    override fun toString(): String {
        return toString { it.toString() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as CaseToken
        if (branches != other.branches) {
            return false
        }
        if (orElse != other.orElse) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = branches.hashCode()
        result = 31 * result + orElse.hashCode()
        return result
    }

    data class Branch(
        val condition: ExpressionToken,
        val thenResult: ExpressionToken
    )
}
