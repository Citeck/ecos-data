package ru.citeck.ecos.data.sql.service.expression.keyword

import ru.citeck.ecos.data.sql.service.expression.ExpressionReader
import ru.citeck.ecos.data.sql.service.expression.ParserStrUtils
import ru.citeck.ecos.data.sql.service.expression.token.CaseToken
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.data.sql.service.expression.token.GroupToken

class CaseKeywordParser : KeywordParser {

    override fun parse(reader: ExpressionReader) {

        val value = reader.value
        val fromIdx = reader.currentIndex()

        val endIdx = value.indexOf("END", ignoreCase = true)
        if (endIdx == -1) {
            reader.throwError("CASE without end")
        }
        val branches = ArrayList<CaseToken.Branch>()
        var idx = fromIdx
        do {
            val whenIdx = ParserStrUtils.indexOf(value, idx, endIdx, listOf("WHEN"), true)
            if (whenIdx == -1) {
                break
            }
            val thenIdx = ParserStrUtils.indexOf(value, whenIdx + 4, endIdx, listOf("THEN"), true)
            val condition = reader.readTokens(whenIdx + 4, thenIdx - 1)
            val endOfThen = ParserStrUtils.indexOf(value, thenIdx + 4, endIdx, listOf("ELSE", "WHEN", "END"), true)
            val expression = reader.readTokens(thenIdx + 4, endOfThen - 1)

            branches.add(
                CaseToken.Branch(
                    GroupToken.wrapIfRequired(condition),
                    GroupToken.wrapIfRequired(expression)
                )
            )
            idx = endOfThen
        } while (true)

        var elseExpression: ExpressionToken? = null
        val elseIdx = ParserStrUtils.indexOf(value, idx, endIdx, listOf("ELSE"), true)
        if (elseIdx != -1) {
            elseExpression = GroupToken.wrapIfRequired(reader.readTokens(elseIdx + 4, endIdx - 1))
        }

        reader.addToken(CaseToken(branches, elseExpression), endIdx + 3)
    }

    override fun getKeyWord(): String {
        return "CASE"
    }
}
