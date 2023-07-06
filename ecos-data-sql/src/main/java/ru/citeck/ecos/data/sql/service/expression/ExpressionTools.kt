package ru.citeck.ecos.data.sql.service.expression

import ru.citeck.ecos.data.sql.service.expression.token.*

object ExpressionTools {

    val AGGREGATE_FUNCTIONS = setOf(
        "max",
        "min",
        "sum",
        "avg",
        "count",
        "bool_and",
        "bool_or",
        "bit_and",
        "bit_or",
    )

    fun <T : ExpressionToken> mapTokens(
        token: ExpressionToken,
        type: Class<T>,
        mapFunc: (T) -> ExpressionToken
    ): ExpressionToken {

        val tokenWithInnerTokensMapped = when (token) {
            is GroupToken -> {
                val newTokens = ArrayList<ExpressionToken>()
                token.tokens.forEach { innerToken ->
                    newTokens.add(mapTokens(innerToken, type, mapFunc))
                }
                GroupToken(newTokens)
            }

            is FunctionToken -> {
                val newArgs = token.args.map { mapTokens(it, type, mapFunc) }
                FunctionToken(token.name, newArgs)
            }
            is CaseToken -> {
                val newBranches = token.branches.map {
                    val condition = mapTokens(it.condition, type, mapFunc)
                    val thenRes = mapTokens(it.thenResult, type, mapFunc)
                    CaseToken.Branch(condition, thenRes)
                }
                val newOrElse = token.orElse?.let { mapTokens(it, type, mapFunc) }
                CaseToken(newBranches, newOrElse)
            }
            else -> token
        }

        return if (type.isInstance(tokenWithInnerTokensMapped)) {
            mapFunc.invoke(type.cast(tokenWithInnerTokensMapped))
        } else {
            tokenWithInnerTokensMapped
        }
    }

    fun resolveAggregateFunctionsForNonGroupedQuery(token: ExpressionToken): ExpressionToken {

        return if (token is GroupToken) {
            val newTokens = ArrayList<ExpressionToken>()
            token.tokens.forEach { innerToken ->
                newTokens.add(resolveAggregateFunctionsForNonGroupedQuery(innerToken))
            }
            GroupToken(newTokens)
        } else if (token is FunctionToken) {
            if (AGGREGATE_FUNCTIONS.contains(token.name) && token.args.size == 1) {
                if (token.name == "count") {
                    if (token.args[0] == AllFieldsToken) {
                        ScalarToken(1L)
                    } else {
                        CaseToken(
                            listOf(
                                CaseToken.Branch(
                                    GroupToken(token.args[0], NullConditionToken(false)),
                                    ScalarToken(1L)
                                )
                            ),
                            ScalarToken(0L)
                        )
                    }
                } else {
                    token.args[0]
                }
            } else {
                val newArgs = token.args.map { resolveAggregateFunctionsForNonGroupedQuery(it) }
                FunctionToken(token.name, newArgs)
            }
        } else if (token is CaseToken) {
            val newBranches = token.branches.map {
                val condition = resolveAggregateFunctionsForNonGroupedQuery(it.condition)
                val thenRes = resolveAggregateFunctionsForNonGroupedQuery(it.thenResult)
                CaseToken.Branch(condition, thenRes)
            }
            val newOrElse = token.orElse?.let { resolveAggregateFunctionsForNonGroupedQuery(it) }
            CaseToken(newBranches, newOrElse)
        } else {
            token
        }
    }
}
