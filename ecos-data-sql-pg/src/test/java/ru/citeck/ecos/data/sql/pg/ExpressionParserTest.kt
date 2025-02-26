package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.service.expression.ExpressionParser
import ru.citeck.ecos.data.sql.service.expression.token.*

class ExpressionParserTest {

    @Test
    fun test() {

        fun assertInvalidExpr(expression: String) {
            assertThrows<RuntimeException>(expression) {
                ExpressionParser.parse(expression)
            }
        }

        fun assertExpr(expression: String, expected: ExpressionToken) {
            val parseRes = try {
                ExpressionParser.parse(expression)
            } catch (e: Throwable) {
                throw RuntimeException("Expression parsing error: '$expression'", e)
            }
            assertThat(parseRes).describedAs(expression).isEqualTo(expected)
        }

        assertInvalidExpr("*")
        assertInvalidExpr("* = *")
        assertInvalidExpr("* * *")
        assertInvalidExpr("sum(*)")
        assertInvalidExpr("func(123)")
        assertInvalidExpr("'qwers''")
        assertInvalidExpr("(")
        assertInvalidExpr(")")
        assertInvalidExpr("([)]")
        assertInvalidExpr("(qq[qq)]")
        assertInvalidExpr("interval '1 month")
        assertInvalidExpr("interval '1 month $'")

        assertExpr(
            "(now()::date AT TIME ZONE 'UTC')",
            GroupToken(
                CastToken(FunctionToken("now", emptyList()), "date"),
                AtTimeZoneToken("UTC")
            )
        )
        assertExpr(
            "(date_trunc('month', _created) + interval '1 month - 1 day')",
            GroupToken(
                FunctionToken("date_trunc", listOf(ScalarToken("month"), ColumnToken("_created"))),
                OperatorToken(OperatorToken.Type.PLUS),
                IntervalToken("1 month - 1 day")
            )
        )
        assertExpr("(now())", GroupToken(FunctionToken("now", emptyList())))
        assertExpr("('qwers')", GroupToken(ScalarToken("qwers")))
        assertExpr("count(*)", FunctionToken("count", listOf(AllFieldsToken)))
        assertExpr("date_trunc(_created)", FunctionToken("date_trunc", listOf(ColumnToken("_created"))))
        assertExpr(
            "date_trunc(_created + 1)",
            FunctionToken(
                "date_trunc",
                listOf(
                    GroupToken(
                        ColumnToken("_created"),
                        OperatorToken(OperatorToken.Type.PLUS),
                        ScalarToken(1L)
                    )
                )
            )
        )

        for (operatorType in OperatorToken.Type.entries) {
            assertExpr(
                "(a ${operatorType.value} b)",
                GroupToken(
                    ColumnToken("a"),
                    OperatorToken(operatorType),
                    ColumnToken("b")
                )
            )
        }

        assertExpr(
            "(CASE WHEN a > 0 AND b < 10 THEN 10 ELSE 5 END)",
            GroupToken(
                CaseToken(
                    listOf(
                        CaseToken.Branch(
                            GroupToken(
                                ColumnToken("a"),
                                OperatorToken(OperatorToken.Type.GREATER),
                                ScalarToken(0L),
                                OperatorToken(OperatorToken.Type.AND),
                                ColumnToken("b"),
                                OperatorToken(OperatorToken.Type.LESS),
                                ScalarToken(10L),
                            ),
                            ScalarToken(10L)
                        )
                    ),
                    ScalarToken(5L)
                )
            )
        )

        assertExpr(
            "((field0 + field1) / 10)",
            GroupToken(
                listOf(
                    GroupToken(
                        listOf(
                            ColumnToken("field0"),
                            OperatorToken(OperatorToken.Type.PLUS),
                            ColumnToken("field1")
                        )
                    ),
                    OperatorToken(OperatorToken.Type.DIV),
                    ScalarToken(10L)
                )
            )
        )

        val expected0 = GroupToken(
            listOf(
                FunctionToken("someFunc", listOf(ColumnToken("someColumn1"), ColumnToken("someColumn2"))),
                OperatorToken(OperatorToken.Type.MULTIPLY),
                ScalarToken("someString"),
                OperatorToken(OperatorToken.Type.PLUS),
                ColumnToken("SomeColumn"),
                OperatorToken(OperatorToken.Type.MULTIPLY),
                ScalarToken(123L),
                OperatorToken(OperatorToken.Type.DIV),
                GroupToken(
                    listOf(
                        ColumnToken("abc"),
                        OperatorToken(OperatorToken.Type.PLUS),
                        ScalarToken(23L),
                        OperatorToken(OperatorToken.Type.MULTIPLY),
                        ColumnToken("qqqqQ"),
                        OperatorToken(OperatorToken.Type.PLUS),
                        GroupToken(
                            listOf(
                                ColumnToken("Qq"),
                                OperatorToken(OperatorToken.Type.PLUS),
                                FunctionToken("count", listOf(AllFieldsToken))
                            )
                        )
                    )
                )
            )
        )
        assertExpr(
            "(someFunc(\"someColumn1\",\"someColumn2\") " +
                "* 'someString'+ \"SomeColumn\" * 123 / (abc + 23*qqqqQ+ (Qq + count(*))))",
            expected0
        )

        assertExpr(
            "(_parent.text = text)",
            GroupToken(
                ColumnToken("_parent.text"),
                OperatorToken(OperatorToken.Type.EQUAL),
                ColumnToken("text")
            )
        )
        assertExpr(
            "(_parent.text <> coalesce(text,''))",
            GroupToken(
                ColumnToken("_parent.text"),
                OperatorToken(OperatorToken.Type.NOT_EQUAL),
                FunctionToken(
                    "coalesce",
                    listOf(ColumnToken("text"), ScalarToken(""))
                )
            )
        )
    }
}
