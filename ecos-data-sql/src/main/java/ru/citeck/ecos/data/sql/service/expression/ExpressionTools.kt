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

    fun mapColumnNames(token: ExpressionToken, mapFunc: (String) -> String): ExpressionToken {

        return when (token) {
            is BracesToken -> {
                val newTokens = ArrayList<ExpressionToken>()
                token.tokens.forEach { innerToken ->
                    newTokens.add(mapColumnNames(innerToken, mapFunc))
                }
                BracesToken(newTokens)
            }
            is FunctionToken -> {
                val newArgs = token.args.map { mapColumnNames(it, mapFunc) }
                FunctionToken(token.name, newArgs)
            }
            is CaseToken -> {
                val newBranches = token.branches.map {
                    val condition = mapColumnNames(it.condition, mapFunc)
                    val thenRes = mapColumnNames(it.thenResult, mapFunc)
                    CaseToken.Branch(condition, thenRes)
                }
                val newOrElse = token.orElse?.let { mapColumnNames(it, mapFunc) }
                CaseToken(newBranches, newOrElse)
            }
            is ColumnToken -> {
                val newColumnName = mapFunc(token.name)
                if (newColumnName != token.name) {
                    ColumnToken(newColumnName)
                } else {
                    token
                }
            }
            else -> {
                token
            }
        }
    }

    fun resolveAggregateFunctionsForNonGroupedQuery(token: ExpressionToken): ExpressionToken {

        return if (token is BracesToken) {
            val newTokens = ArrayList<ExpressionToken>()
            token.tokens.forEach { innerToken ->
                newTokens.add(resolveAggregateFunctionsForNonGroupedQuery(innerToken))
            }
            BracesToken(newTokens)
        } else if (token is FunctionToken) {
            if (AGGREGATE_FUNCTIONS.contains(token.name) && token.args.size == 1) {
                if (token.name == "count") {
                    if (token.args[0] == AllFieldsToken) {
                        ScalarToken(1L)
                    } else {
                        CaseToken(
                            listOf(
                                CaseToken.Branch(
                                    BracesToken(token.args[0], NullConditionToken(false)),
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
