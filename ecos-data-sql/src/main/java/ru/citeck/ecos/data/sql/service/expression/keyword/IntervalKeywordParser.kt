package ru.citeck.ecos.data.sql.service.expression.keyword

import ru.citeck.ecos.data.sql.service.expression.ExpressionReader
import ru.citeck.ecos.data.sql.service.expression.ParserStrUtils
import ru.citeck.ecos.data.sql.service.expression.token.IntervalToken

class IntervalKeywordParser : KeywordParser {

    companion object {
        private const val KEYWORD = "INTERVAL"
    }

    override fun parse(reader: ExpressionReader) {

        val fromIdx = reader.currentIndex()
        val value = reader.value

        var startIdx = fromIdx + KEYWORD.length + 1
        while (startIdx < value.length && value[startIdx].isWhitespace()) {
            startIdx++
        }
        if (startIdx >= value.length) {
            error("Invalid interval at index $fromIdx")
        }
        val endIdx = ParserStrUtils.indexOf(value, startIdx + 1, value.length) { _, ch -> ch == '\'' }
        if (endIdx == -1) {
            error("Invalid interval at index $fromIdx")
        }
        reader.addToken(IntervalToken(value.substring(startIdx + 1, endIdx)), endIdx + 1)
    }

    override fun getKeyWord(): String {
        return KEYWORD
    }
}
