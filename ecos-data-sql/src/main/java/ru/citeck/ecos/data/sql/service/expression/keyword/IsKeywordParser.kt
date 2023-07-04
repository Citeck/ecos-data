package ru.citeck.ecos.data.sql.service.expression.keyword

import ru.citeck.ecos.data.sql.service.expression.ExpressionReader
import ru.citeck.ecos.data.sql.service.expression.ParserStrUtils
import ru.citeck.ecos.data.sql.service.expression.token.NullConditionToken

class IsKeywordParser : KeywordParser {

    override fun parse(reader: ExpressionReader) {

        val value = reader.value
        val fromIdx = reader.currentIndex()

        val nullIdx = ParserStrUtils.indexOf(value, fromIdx, value.length - 1, listOf("NULL"), true)
        if (nullIdx == -1) {
            reader.throwError()
        }
        val conditionParts = value.substring(fromIdx, nullIdx + 4).split(" ")
        when (conditionParts.size) {
            3 -> {
                if (!conditionParts[0].equals("IS", true) ||
                    !conditionParts[1].equals("NOT", true) ||
                    !conditionParts[2].equals("NULL", true)
                ) {
                    error("Invalid expression '$value' at index $fromIdx")
                }
                reader.addToken(NullConditionToken(false), nullIdx + 4)
            }
            2 -> {
                if (!conditionParts[0].equals("IS", true) ||
                    !conditionParts[1].equals("NULL", true)
                ) {
                    error("Invalid expression: $value")
                }
                reader.addToken(NullConditionToken(true), nullIdx + 4)
            }
            else -> {
                reader.throwError("Unexpected expression parts count ${conditionParts.size} but expected 2 or 3")
            }
        }
    }

    override fun getKeyWord(): String {
        return "IS"
    }
}
