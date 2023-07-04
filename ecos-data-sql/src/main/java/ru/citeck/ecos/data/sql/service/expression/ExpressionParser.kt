package ru.citeck.ecos.data.sql.service.expression

import ru.citeck.ecos.data.sql.service.expression.keyword.AtTimeZoneParser
import ru.citeck.ecos.data.sql.service.expression.keyword.CaseKeywordParser
import ru.citeck.ecos.data.sql.service.expression.keyword.IntervalKeywordParser
import ru.citeck.ecos.data.sql.service.expression.keyword.IsKeywordParser
import ru.citeck.ecos.data.sql.service.expression.token.*

object ExpressionParser {

    val KEY_WORD_PARSERS = listOf(
        IsKeywordParser(),
        IntervalKeywordParser(),
        CaseKeywordParser(),
        AtTimeZoneParser()
    ).associateBy { it.getKeyWord().uppercase() }

    fun parse(value: String): ExpressionToken {
        val reader = ExpressionReader(value)
        reader.readToken(value.lastIndex)
        val token = reader.getTokens().first()
        if (token !is FunctionToken && token !is GroupToken) {
            error("Invalid expression: '$value'")
        }
        token.validate()
        return token
    }
}
