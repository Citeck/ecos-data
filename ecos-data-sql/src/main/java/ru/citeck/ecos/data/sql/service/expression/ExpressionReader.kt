package ru.citeck.ecos.data.sql.service.expression

import ru.citeck.ecos.data.sql.service.expression.token.*

class ExpressionReader(val value: String) {

    private var currentIdx: Int = 0
    private var lastIdx: Int = value.length - 1

    private var tokens: MutableList<ExpressionToken> = ArrayList()

    fun getTokens(): List<ExpressionToken> {
        return tokens
    }

    fun readTokens(fromIdx: Int, toIdx: Int): List<ExpressionToken> {
        if (fromIdx < currentIdx) {
            throwError(
                "Group fromIdx can't be less than current index. " +
                    "fromIdx = $fromIdx currentIdx = $currentIdx"
            )
        }
        this.currentIdx = fromIdx
        val prevLastIdx = this.lastIdx
        this.lastIdx = toIdx
        val prevTokens = tokens
        this.tokens = ArrayList()

        try {
            var firstIteration = true
            while (currentIdx <= lastIdx) {
                currentIdx = ParserStrUtils.indexOf(value, currentIdx, lastIdx) { _, ch -> !ch.isWhitespace() }
                if (currentIdx == -1) {
                    currentIdx = lastIdx
                    return tokens
                }
                val idxAfterOperator = if (firstIteration) {
                    currentIdx
                } else {
                    OperatorToken.getIdxAfterOperator(value, currentIdx)
                }
                if (idxAfterOperator != currentIdx) {
                    tokens.add(OperatorToken.valueOf(value.substring(currentIdx, idxAfterOperator)))
                    currentIdx = idxAfterOperator
                } else {
                    var tokenEnd = ParserStrUtils.indexOf(value, currentIdx, toIdx) { _, ch ->
                        ch.isWhitespace() || (!firstIteration && OperatorToken.isOperatorChar(ch))
                    }
                    if (tokenEnd == -1) {
                        tokenEnd = toIdx + 1
                    }
                    readToken(tokenEnd - 1)
                    currentIdx += 1
                }
                firstIteration = false
            }
            return tokens
        } finally {
            this.tokens = prevTokens
            this.lastIdx = prevLastIdx
        }
    }

    fun readToken(toIdx: Int) {
        if (toIdx > this.lastIdx) {
            throwError(
                "Token lastIdx can't be greater than current last index. " +
                    "lastIdx = $toIdx current lastIdx = ${this.lastIdx}"
            )
        }
        while (currentIdx < value.length && value[currentIdx].isWhitespace()) {
            currentIdx++
        }
        val prevLastIdx = this.lastIdx
        this.lastIdx = toIdx
        while (lastIdx > currentIdx && value[lastIdx].isWhitespace()) {
            lastIdx--
        }

        val keyWordParser = ExpressionParser.KEY_WORD_PARSERS.entries.find {
            ParserStrUtils.startsWith(value, currentIdx, it.key, true)
        }?.value

        if (keyWordParser != null) {
            keyWordParser.parse(this)
            return
        }

        var castTo = ""
        val castDelimIdx = ParserStrUtils.indexOf(
            value,
            currentIdx,
            lastIdx,
            listOf(CastToken.DELIMITER),
            false
        )
        if (castDelimIdx != -1) {
            castTo = value.substring(castDelimIdx + 2, lastIdx + 1)
            lastIdx = castDelimIdx - 1
        }

        fun addSimpleToken(token: ExpressionToken) {
            if (castTo.isNotEmpty()) {
                tokens.add(CastToken(token, castTo))
            } else {
                tokens.add(token)
            }
            currentIdx = toIdx
        }

        var error: Throwable? = null
        try {
            if (currentIdx == lastIdx && value[currentIdx] == '*') {
                addSimpleToken(AllFieldsToken)
            } else if (value[currentIdx] == '(' && value[lastIdx] == ')') {
                addSimpleToken(GroupToken(readTokens(currentIdx + 1, lastIdx - 1)))
            } else {

                if (value[currentIdx].isLetter() && value[lastIdx] == ')') {

                    val argsOpenBraceIdx =
                        ParserStrUtils.indexOf(value, currentIdx + 1, lastIdx) { _, ch -> ch == '(' }
                    val name = value.substring(currentIdx, argsOpenBraceIdx)
                    val args = mutableListOf<ExpressionToken>()

                    val argsFirstIdx = argsOpenBraceIdx + 1
                    val argsLastIdx = lastIdx - 1
                    if (argsFirstIdx <= argsLastIdx) {
                        ParserStrUtils.split(value, argsFirstIdx, argsLastIdx, ',') { innerFrom, innerTo ->
                            if (innerFrom == innerTo && value[innerFrom] == '*') {
                                args.add(AllFieldsToken)
                            } else {
                                val tokens = readTokens(innerFrom, innerTo)
                                if (tokens.size == 1) {
                                    args.add(tokens[0])
                                } else {
                                    args.add(GroupToken(tokens))
                                }
                            }
                        }
                    }
                    addSimpleToken(FunctionToken(name, args))
                } else if (value[currentIdx] == '\'' && value[lastIdx] == '\'') {
                    addSimpleToken(ScalarToken(value.substring(currentIdx + 1, lastIdx)))
                } else if (value[currentIdx] == '"' && value[lastIdx] == '"') {
                    addSimpleToken(ColumnToken(value.substring(currentIdx + 1, lastIdx)))
                } else {
                    val textValue = value.substring(currentIdx, lastIdx + 1)
                    val firstChar = textValue.first()
                    if (firstChar.isLetter() || firstChar == '_') {
                        addSimpleToken(
                            when (textValue) {
                                "true" -> ScalarToken(true)
                                "false" -> ScalarToken(false)
                                "null" -> NullToken
                                else -> ColumnToken(textValue)
                            }
                        )
                    } else if (firstChar.isDigit() || firstChar == '-' || firstChar == '+') {
                        if (textValue.contains(".")) {
                            addSimpleToken(ScalarToken(textValue.toDouble()))
                        } else {
                            addSimpleToken(ScalarToken(textValue.toLong()))
                        }
                    } else {
                        throwError()
                    }
                }
            }
        } catch (e: Throwable) {
            error = e
        } finally {
            if (error == null) {
                if (currentIdx < toIdx) {
                    throwError("Current index doesn't incremented")
                }
            } else {
                throw error
            }
            this.lastIdx = prevLastIdx
        }
    }

    fun currentIndex(): Int {
        return currentIdx
    }

    fun lastIndex(): Int {
        return lastIdx
    }

    fun addToken(token: ExpressionToken, newIndex: Int) {
        tokens.add(token)
        this.currentIdx = newIndex
    }

    fun throwError(msg: String = "") {
        var errorMsg = "Parsing completed with error for '$value'[$currentIdx:$lastIdx]"
        if (msg.isNotEmpty()) {
            errorMsg += " with message: $msg"
        }
        error(errorMsg)
    }
}
