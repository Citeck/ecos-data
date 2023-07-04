package ru.citeck.ecos.data.sql.service.expression.keyword

import ru.citeck.ecos.data.sql.service.expression.ExpressionReader
import ru.citeck.ecos.data.sql.service.expression.ParserStrUtils
import ru.citeck.ecos.data.sql.service.expression.token.AtTimeZoneToken

class AtTimeZoneParser : KeywordParser {

    override fun parse(reader: ExpressionReader) {
        val value = reader.value
        val idx = reader.currentIndex()

        val tzStart = ParserStrUtils.indexOf(value, idx, value.length) { _, ch -> ch == '\'' }
        if (tzStart == -1) {
            reader.throwError()
        }
        var wordsBeforeTxEndIdx = tzStart - 1
        while (wordsBeforeTxEndIdx > idx && value[wordsBeforeTxEndIdx].isWhitespace()) {
            wordsBeforeTxEndIdx--
        }

        val parts = value.substring(idx, wordsBeforeTxEndIdx + 1).split(" ")
        if (parts.size != 3 ||
            !parts[0].equals("AT", true) ||
            !parts[1].equals("TIME", true) ||
            !parts[2].equals("ZONE", true)
        ) {
            reader.throwError("Parts: $parts")
        }

        val tzEnd = ParserStrUtils.indexOf(value, tzStart + 1, value.length) { _, ch -> ch == '\'' }
        if (tzEnd == -1) {
            reader.throwError()
        }
        reader.addToken(AtTimeZoneToken(value.substring(tzStart + 1, tzEnd)), tzEnd)
    }

    override fun getKeyWord(): String {
        return "AT"
    }
}
