package ru.citeck.ecos.data.sql.records.dao.atts

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.time.TimeZoneContext
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.records.dao.query.DbFindQueryContext
import ru.citeck.ecos.data.sql.records.dao.query.DbQueryPreparingException
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.data.sql.service.expression.ExpressionParser
import ru.citeck.ecos.data.sql.service.expression.ExpressionTools
import ru.citeck.ecos.data.sql.service.expression.token.*
import java.time.Duration
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger

class DbExpressionAttsContext(
    private val queryContext: DbFindQueryContext,
    private val withGrouping: Boolean
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val expressionCounter = AtomicInteger()
    private val expressionAliases = HashMap<ExpressionToken, String>()
    private val expressionAliasesByAttribute = LinkedHashMap<String, String>()
    private val expressions: MutableMap<String, ExpressionToken> = LinkedHashMap()

    fun register(attribute: String, strict: Boolean): String? {
        var expression = try {
            ExpressionParser.parse(attribute)
        } catch (e: Throwable) {
            val msg = "Invalid expression: '$attribute'"
            if (strict) {
                throw DbQueryPreparingException(msg)
            } else {
                log.debug { msg }
                return null
            }
        }
        if (!withGrouping) {
            var expressionProcessed = false
            if (expression is FunctionToken &&
                ExpressionTools.AGGREGATE_FUNCTIONS.contains(expression.name) &&
                expression.args.size == 1 &&
                expression.args[0] is ColumnToken
            ) {
                val column = (expression.args[0] as ColumnToken)
                val dotIdx = column.name.indexOf('.')
                if (dotIdx > 0) {
                    val assocAtt = column.name.substring(0, dotIdx)
                    val innerAtt = column.name.substring(dotIdx + 1)
                    if (!innerAtt.all { it == ':' || it in 'a'..'z' || it in 'A'..'Z' }) {
                        error("invalid inner attribute: $innerAtt")
                    }

                    val assocRecordsCtx = queryContext.getAssocRecordsCtxToJoin(assocAtt)
                        ?: error("Invalid expression: '$attribute'")

                    if (!innerAtt.contains(".") && assocRecordsCtx.getDbColumnByName(innerAtt) == null) {

                        expression = ExpressionTools.resolveAggregateFunctionForNonExistentColumn(expression)
                    } else {

                        val innerExpression = FunctionToken(
                            expression.name,
                            listOf(ColumnToken(innerAtt))
                        )

                        val tableContext = assocRecordsCtx.dataService.getTableContext()
                        val attributeId = tableContext.getAssocsService().getIdForAtt(assocAtt)

                        expression = AssocAggregationSelectExpression(
                            assocRecordsCtx.dataService.getTableContext(),
                            innerExpression,
                            attributeId
                        )
                    }
                    expressionProcessed = true
                }
            }
            if (!expressionProcessed) {
                expression = ExpressionTools.resolveAggregateFunctionsForNonGroupedQuery(expression)
            }
        }
        expression = ExpressionTools.mapTokens(expression, ExpressionToken::class.java) { token ->
            if (token is ColumnToken) {
                val newColumnName = DbRecord.ATTS_MAPPING.getOrDefault(token.name, token.name)
                val resToken = if (newColumnName != token.name) {
                    ColumnToken(newColumnName)
                } else {
                    token
                }
                if (resToken.name.contains('.')) {
                    val newName = queryContext.prepareAssocSelectJoin(resToken.name, false)
                    if (newName.isNullOrBlank()) {
                        NullToken
                    } else {
                        ColumnToken(newName)
                    }
                } else {
                    if (!queryContext.getTableContext().hasColumn(resToken.name)) {
                        NullToken
                    } else {
                        resToken
                    }
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

                    dateExpression = applyTimezoneOffset(dateExpression)

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

    private fun applyTimezoneOffset(dateExpression: ExpressionToken): ExpressionToken {
        val offset = TimeZoneContext.getUtcOffset()
        if (offset == Duration.ZERO) {
            return dateExpression
        }

        val timezoneStr = ZoneOffset.ofTotalSeconds(offset.seconds.toInt()).id
        return GroupToken(dateExpression, AtTimeZoneToken(timezoneStr))
    }

    class AssocAggregationSelectExpression(
        val tableContext: DbTableContext,
        val expression: ExpressionToken,
        val attributeId: Long
    ) : ExpressionToken {

        override fun validate() {
            expression.validate()
        }

        override fun <T : ExpressionToken> visit(type: Class<T>, visitor: (T) -> Unit) {
        }
    }
}
