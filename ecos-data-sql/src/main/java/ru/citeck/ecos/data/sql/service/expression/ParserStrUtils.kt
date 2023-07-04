package ru.citeck.ecos.data.sql.service.expression

object ParserStrUtils {

    fun startsWith(value: String, fromIdx: Int, substring: String, ignoreCase: Boolean): Boolean {
        for (idx in substring.indices) {
            if (!value[fromIdx + idx].equals(substring[idx], ignoreCase)) {
                return false
            }
        }
        return true
    }

    fun split(value: String, fromIdx: Int, toIdx: Int, delimiter: Char, action: (Int, Int) -> Unit) {
        return split(value, fromIdx, toIdx, { _, ch -> ch == delimiter }, action)
    }

    fun split(
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
                startIdx =
                    indexOf(value, delimIdx + 1, toIdx) { idx, ch -> !delimiter.invoke(idx, ch) }
                if (startIdx == -1) {
                    break
                }
            }
        } while (delimIdx != -1)
    }

    fun indexOf(
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

    fun indexOf(value: String, fromIdx: Int, toIdx: Int, predicate: (Int, Char) -> Boolean): Int {
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
