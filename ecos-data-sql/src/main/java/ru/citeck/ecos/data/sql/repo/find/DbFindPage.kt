package ru.citeck.ecos.data.sql.repo.find

data class DbFindPage(
    val skipCount: Int,
    val maxItems: Int
) {
    companion object {
        val ALL = DbFindPage(0, -1)
    }
}
