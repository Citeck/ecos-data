package ru.citeck.ecos.data.sql.service.expression.keyword

import ru.citeck.ecos.data.sql.service.expression.ExpressionReader

interface KeywordParser {

    fun parse(reader: ExpressionReader)

    fun getKeyWord(): String
}
