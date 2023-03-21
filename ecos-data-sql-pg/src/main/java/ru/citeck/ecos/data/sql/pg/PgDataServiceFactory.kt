package ru.citeck.ecos.data.sql.pg

import org.postgresql.jdbc.PgArray
import org.postgresql.util.PGobject
import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter

class PgDataServiceFactory : DbDataServiceFactory {

    override fun registerConverters(typesConverter: DbTypesConverter) {
        typesConverter.register(PgArray::class) { it.array }
        typesConverter.register(PGobject::class) { it.value }
    }

    override fun createSchemaDao(): DbSchemaDao {
        return DbSchemaDaoPg()
    }

    override fun createEntityRepo(): DbEntityRepo {
        return DbEntityRepoPg()
    }
}
