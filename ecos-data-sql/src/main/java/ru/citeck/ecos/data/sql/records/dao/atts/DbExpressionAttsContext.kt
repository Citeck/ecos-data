package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.expression.ExpressionParser
import ru.citeck.ecos.data.sql.service.expression.ExpressionTools
import ru.citeck.ecos.data.sql.service.expression.token.*
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger

class DbExpressionAttsContext(
    private val withGrouping: Boolean
) {

    private val expressionCounter = AtomicInteger()
    private val expressionAliases = HashMap<ExpressionToken, String>()
    private val expressionAliasesByAttribute = LinkedHashMap<String, String>()
    private val expressions: MutableMap<String, ExpressionToken> = LinkedHashMap()

    fun register(attribute: String): String {
        var expression = ExpressionParser.parse(attribute)
        if (!withGrouping) {
            expression = ExpressionTools.resolveAggregateFunctionsForNonGroupedQuery(expression)
        }
        expression = ExpressionTools.mapTokens(expression, ExpressionToken::class.java) { token ->
            if (token is ColumnToken) {
                val newColumnName = DbRecord.ATTS_MAPPING.getOrDefault(token.name, token.name)
                if (newColumnName != token.name) {
                    ColumnToken(newColumnName)
                } else {
                    token
                }
            } else if (token is FunctionToken) {
                if (token.name == "startOfMonth" || token.name == "endOfMonth") {
                    if (token.args.size != 1) {
                        error("Invalid function arguments: $token")
                    }
                    val arg = (token.args[0] as? ScalarToken)?.value as? Long
                        ?: error("Invalid function arguments: $token")

                    var dateExpression: ExpressionToken = GroupToken(
                        FunctionToken("current_date", emptyList())
                    )
                    if (arg != 0L) {
                        dateExpression = GroupToken(
                            dateExpression,
                            OperatorToken(OperatorToken.Type.PLUS),
                            IntervalToken("$arg month")
                        )
                    }
                    if (token.name == "startOfMonth") {
                        CastToken(
                            FunctionToken(
                                "date_trunc",
                                listOf(ScalarToken("month"), dateExpression)
                            ),
                            "date"
                        )
                    } else {
                        CastToken(
                            GroupToken(
                                FunctionToken(
                                    "date_trunc",
                                    listOf(ScalarToken("month"), dateExpression)
                                ),
                                OperatorToken(OperatorToken.Type.PLUS),
                                IntervalToken("1 month - 1 day")
                            ),
                            "date"
                        )
                    }
                } else {
                    token
                }
            } else {
                token
            }
        }

        val alias = expressionAliases.computeIfAbsent(expression) {
            val newAlias = "expr_${expressionCounter.getAndIncrement()}"
            expressions[newAlias] = expression
            newAlias
        }
        expressionAliasesByAttribute[attribute] = alias
        return alias
    }

    fun getExpressions(): Map<String, ExpressionToken> {
        return expressions
    }

    fun mapEntityAtts(entity: DbEntity): DbEntity {
        if (expressionAliasesByAttribute.isEmpty()) {
            return entity
        }
        val mappedEntity = entity.copy()
        expressionAliasesByAttribute.forEach { attAndAlias ->
            mappedEntity.attributes[attAndAlias.key] = mappedEntity.attributes[attAndAlias.value]
        }
        expressionAliasesByAttribute.values.toSet().forEach { alias ->
            mappedEntity.attributes.remove(alias)
        }
        return mappedEntity
    }

    fun mapEntitiesAtts(entities: List<DbEntity>): List<DbEntity> {
        return if (expressionAliasesByAttribute.isEmpty()) {
            entities
        } else {
            entities.map { mapEntityAtts(it) }
        }
    }
}
