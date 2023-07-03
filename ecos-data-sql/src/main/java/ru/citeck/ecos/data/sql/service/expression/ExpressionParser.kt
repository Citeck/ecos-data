package ru.citeck.ecos.data.sql.service.expression

import ru.citeck.ecos.data.sql.service.expression.token.*

object ExpressionParser {

    fun parse(value: String): ExpressionToken {
        val token = parseToken(value, 0, value.lastIndex).first
        if (token !is FunctionToken && token !is BracesToken) {
            error("Invalid expression: '$value'")
        }
        token.validate()
        return token
    }

    private fun parseToken(value: String, fromIdx: Int, toIdx: Int): Pair<ExpressionToken, Int> {
        if (value.isBlank()) {
            error("Invalid token value: '$value'")
        }
        var fromIdxNws = fromIdx
        while (fromIdxNws < toIdx && value[fromIdxNws].isWhitespace()) {
            fromIdxNws++
        }
        var toIdxNws = toIdx
        while (toIdxNws > fromIdxNws && value[toIdxNws].isWhitespace()) {
            toIdxNws--
        }
        if (fromIdxNws > toIdxNws) {
            error("Invalid token for value '$value'[$fromIdx:$toIdx]")
        }
        if (fromIdxNws == toIdxNws && value[fromIdxNws] == '*') {
            return AllFieldsToken to toIdx
        }
        return if (value[fromIdxNws] == '(' && value[toIdxNws] == ')') {

            BracesToken(parseTokens(value, fromIdxNws + 1, toIdxNws - 1)) to toIdx
        } else {

            if (value[fromIdxNws].isLetter() && value[toIdxNws] == ')') {

                val argsOpenBraceIdx = indexOf(value, fromIdxNws + 1, toIdxNws) { _, ch -> ch == '(' }
                val name = value.substring(fromIdxNws, argsOpenBraceIdx)
                val args = mutableListOf<ExpressionToken>()

                val argsFirstIdx = argsOpenBraceIdx + 1
                val argsLastIdx = toIdxNws - 1
                if (argsFirstIdx <= argsLastIdx) {
                    split(value, argsFirstIdx, argsLastIdx, ',') { innerFrom, innerTo ->
                        args.add(parseToken(value, innerFrom, innerTo).first)
                    }
                }
                FunctionToken(name, args) to toIdx
            } else if (value[fromIdxNws] == '\'' && value[toIdxNws] == '\'') {
                ScalarToken(value.substring(fromIdxNws + 1, toIdxNws)) to toIdx
            } else if (value[fromIdxNws] == '"' && value[toIdxNws] == '"') {
                ColumnToken(value.substring(fromIdxNws + 1, toIdxNws)) to toIdx
            } else {
                val textValue = value.substring(fromIdxNws, toIdxNws + 1)
                val firstChar = textValue.first()
                if (firstChar.isLetter() || firstChar == '_') {
                    if (textValue.equals("CASE", true)) {
                        parseCaseToken(value, fromIdxNws)
                    } else if (textValue.equals("IS", true)) {
                        parseIsCondition(value, fromIdxNws)
                    } else {
                        when (textValue) {
                            "true" -> ScalarToken(true)
                            "false" -> ScalarToken(false)
                            "null" -> NullToken
                            else -> ColumnToken(textValue)
                        } to toIdx
                    }
                } else if (firstChar.isDigit()) {
                    if (textValue.contains(".")) {
                        ScalarToken(textValue.toDouble())
                    } else {
                        ScalarToken(textValue.toLong())
                    } to toIdx
                } else {
                    error("Invalid term: '$textValue' in value '$value'[$fromIdxNws:$toIdxNws]")
                }
            }
        }
    }

    private fun parseIsCondition(value: String, fromIdx: Int): Pair<NullConditionToken, Int> {
        val nullIdx = indexOf(value, fromIdx, value.length - 1, listOf("NULL"), true)
        if (nullIdx == -1) {
            error("Invalid expression: $value")
        }
        val conditionParts = value.substring(fromIdx, nullIdx + 4).split(" ")
        return when (conditionParts.size) {
            3 -> {
                if (!conditionParts[0].equals("IS", true) ||
                    !conditionParts[1].equals("NOT", true) ||
                    !conditionParts[2].equals("NULL", true)
                ) {
                    error("Invalid expression: $value")
                }
                NullConditionToken(false) to nullIdx + 4
            }
            2 -> {
                if (!conditionParts[0].equals("IS", true) ||
                    !conditionParts[1].equals("NULL", true)
                ) {
                    error("Invalid expression: $value")
                }
                NullConditionToken(true) to nullIdx + 4
            }
            else -> {
                error("Invalid expression: $value")
            }
        }
    }

    private fun parseCaseToken(value: String, fromIdx: Int): Pair<CaseToken, Int> {

        val endIdx = value.indexOf("END", ignoreCase = true)
        if (endIdx == -1) {
            error("CASE without end")
        }
        val branches = ArrayList<CaseToken.Branch>()
        var idx = fromIdx
        do {
            val whenIdx = indexOf(value, idx, endIdx, listOf("WHEN"), true)
            if (whenIdx == -1) {
                break
            }
            val thenIdx = indexOf(value, whenIdx + 4, endIdx, listOf("THEN"), true)
            val condition = parseTokens(value, whenIdx + 4, thenIdx - 1)
            val endOfThen = indexOf(value, thenIdx + 4, endIdx, listOf("ELSE", "WHEN", "END"), true)
            val expression = parseToken(value, thenIdx + 4, endOfThen - 1).first
            branches.add(CaseToken.Branch(BracesToken(condition), expression))
            idx = endOfThen
        } while (true)

        var elseExpression: ExpressionToken? = null
        val elseIdx = indexOf(value, idx, endIdx, listOf("ELSE"), true)
        if (elseIdx != -1) {
            elseExpression = parseToken(value, elseIdx + 4, endIdx - 1).first
        }

        return CaseToken(branches, elseExpression) to endIdx + 3
    }

    private fun split(value: String, fromIdx: Int, toIdx: Int, delimiter: Char, action: (Int, Int) -> Unit) {
        return split(value, fromIdx, toIdx, { _, ch -> ch == delimiter }, action)
    }

    private fun split(
        value: String,
        fromIdx: Int,
        toIdx: Int,
        delimiter: (Int, Char) -> Boolean,
        action: (Int, Int) -> Unit
    ) {
        var startIdx = fromIdx
        do {
            val delimIdx = indexOf(value, startIdx, toIdx, delimiter)
            if (delimIdx == -1) {
                if (startIdx <= toIdx) {
                    action.invoke(startIdx, toIdx)
                }
            } else {
                action.invoke(startIdx, delimIdx - 1)
                startIdx = indexOf(value, delimIdx + 1, toIdx) { idx, ch -> !delimiter.invoke(idx, ch) }
                if (startIdx == -1) {
                    break
                }
            }
        } while (delimIdx != -1)
    }

    private fun parseTokens(value: String, fromIdx: Int, toIdx: Int): List<ExpressionToken> {

        val tokens = mutableListOf<ExpressionToken>()

        var idx = fromIdx
        while (idx <= toIdx) {
            idx = indexOf(value, idx, toIdx) { _, ch -> !ch.isWhitespace() }
            if (idx == -1) {
                return tokens
            }
            val idxAfterOperator = OperatorToken.getIdxAfterOperator(value, idx)
            if (idxAfterOperator != idx) {
                tokens.add(OperatorToken.valueOf(value.substring(idx, idxAfterOperator)))
                idx = idxAfterOperator
            } else {
                var tokenEnd = indexOf(value, idx, toIdx) { _, ch ->
                    ch.isWhitespace() || OperatorToken.isOperatorChar(ch)
                }
                if (tokenEnd == -1) {
                    tokenEnd = toIdx + 1
                }
                val parseRes = parseToken(value, idx, tokenEnd - 1)
                tokens.add(parseRes.first)
                idx = parseRes.second + 1
            }
        }
        return tokens
    }

    private fun indexOf(
        value: String,
        fromIdx: Int,
        toIdx: Int,
        substrings: Collection<String>,
        ignoreCase: Boolean
    ): Int {
        return indexOf(value, fromIdx, toIdx) { idx, ch ->
            var match = true
            for (substring in substrings) {
                if (ch.equals(substring[0], ignoreCase) && (idx + substring.length) <= value.length) {
                    for (i in substring.indices) {
                        if (!value[idx + i].equals(substring[i], ignoreCase)) {
                            match = false
                            break
                        }
                    }
                } else {
                    match = false
                }
                if (match) {
                    break
                }
            }
            match
        }
    }

    private fun indexOf(value: String, fromIdx: Int, toIdx: Int, predicate: (Int, Char) -> Boolean): Int {
        if (fromIdx > toIdx) {
            return -1
        }
        var openCtxChar = ' '
        var closeCtxChar = ' '
        var idx = fromIdx
        while (idx <= toIdx) {
            val currentChar = value[idx]
            if (openCtxChar != ' ') {
                if (currentChar == closeCtxChar) {
                    openCtxChar = ' '
                    closeCtxChar = ' '
                } else if (isOpenContextChar(currentChar)) {
                    val innerCloseCtxChar = getCloseCtxChar(currentChar)
                    idx = indexOf(value, idx + 1, toIdx) { _, ch -> ch == innerCloseCtxChar }
                    if (idx == -1) {
                        return -1
                    }
                }
            } else {
                if (predicate.invoke(idx, currentChar)) {
                    return idx
                }
                if (isOpenContextChar(currentChar)) {
                    openCtxChar = currentChar
                    closeCtxChar = getCloseCtxChar(openCtxChar)
                }
            }
            idx++
        }
        return -1
    }

    private fun isOpenContextChar(ch: Char): Boolean {
        return ch == '\'' || ch == '"' || ch == '(' || ch == '{' || ch == '['
    }

    private fun getCloseCtxChar(openChar: Char): Char {
        return when (openChar) {
            '\'' -> '\''
            '\"' -> '\"'
            '(' -> ')'
            '[' -> ']'
            '{' -> '}'
            else -> ' '
        }
    }
}
