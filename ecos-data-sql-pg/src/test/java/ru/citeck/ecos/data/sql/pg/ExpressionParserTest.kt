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
            assertThat(ExpressionParser.parse(expression)).describedAs(expression).isEqualTo(expected)
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

        assertExpr("(now())", BracesToken(FunctionToken("now", emptyList())))
        assertExpr("('qwers')", BracesToken(ScalarToken("qwers")))
        assertExpr("count(*)", FunctionToken("count", listOf(AllFieldsToken)))
        assertExpr("date_trunc(_created)", FunctionToken("date_trunc", listOf(ColumnToken("_created"))))

        for (operatorType in OperatorToken.Type.values()) {
            assertExpr(
                "(a ${operatorType.value} b)",
                BracesToken(
                    ColumnToken("a"),
                    OperatorToken(operatorType),
                    ColumnToken("b")
                )
            )
        }

        assertExpr(
            "(CASE WHEN a > 0 AND b < 10 THEN 10 ELSE 5 END)",
            BracesToken(
                CaseToken(
                    listOf(
                        CaseToken.Branch(
                            BracesToken(
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
            BracesToken(
                listOf(
                    BracesToken(
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

        val expected0 = BracesToken(
            listOf(
                FunctionToken("someFunc", listOf(ColumnToken("someColumn1"), ColumnToken("someColumn2"))),
                OperatorToken(OperatorToken.Type.MULTIPLY),
                ScalarToken("someString"),
                OperatorToken(OperatorToken.Type.PLUS),
                ColumnToken("SomeColumn"),
                OperatorToken(OperatorToken.Type.MULTIPLY),
                ScalarToken(123L),
                OperatorToken(OperatorToken.Type.DIV),
                BracesToken(
                    listOf(
                        ColumnToken("abc"),
                        OperatorToken(OperatorToken.Type.PLUS),
                        ScalarToken(23L),
                        OperatorToken(OperatorToken.Type.MULTIPLY),
                        ColumnToken("qqqqQ"),
                        OperatorToken(OperatorToken.Type.PLUS),
                        BracesToken(
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
    }
}
