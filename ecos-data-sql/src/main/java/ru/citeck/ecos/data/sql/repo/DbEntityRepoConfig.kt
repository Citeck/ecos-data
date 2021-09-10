package ru.citeck.ecos.data.sql.repo

import ru.citeck.ecos.data.sql.dto.DbTableRef

class DbEntityRepoConfig(
    val authEnabled: Boolean,
    val permsTable: DbTableRef,
    val authoritiesTable: DbTableRef
)
