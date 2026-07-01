package ru.citeck.ecos.data.sql.inmem

import ru.citeck.ecos.data.sql.repo.DbEntityRepo
import ru.citeck.ecos.data.sql.schema.DbSchemaDao
import ru.citeck.ecos.data.sql.service.DbDataServiceFactory
import ru.citeck.ecos.data.sql.type.DbTypesConverter

/**
 * In-memory [DbDataServiceFactory]: the single seam that selects the in-memory storage backend.
 *
 * Pass an instance of this factory (together with an
 * [ru.citeck.ecos.data.sql.inmem.datasource.InMemDataSource]) to
 * [ru.citeck.ecos.data.sql.context.DbDataSourceContext] and everything above the storage SPI
 * ([ru.citeck.ecos.data.sql.service.DbDataServiceImpl], `DbRecordsDao`, ...) works unchanged.
 *
 * Unlike [ru.citeck.ecos.data.sql.pg.PgDataServiceFactory] there are no `PGobject`/`PgArray`
 * converters to register: the in-memory backend never produces PostgreSQL wrapper types, it stores
 * plain JVM values. The default converters of [DbTypesConverter] (numbers, dates, [ru.citeck.ecos.commons.data.MLText],
 * URIs, arrays) cover the plain-JVM equivalents.
 */
class InMemDataServiceFactory : DbDataServiceFactory {

    override fun registerConverters(typesConverter: DbTypesConverter) {
        // no backend-specific converters: in-memory rows hold plain JVM values and the default
        // DbTypesConverter registrations already handle every common type.
    }

    override fun createSchemaDao(): DbSchemaDao {
        return InMemSchemaDao()
    }

    override fun createEntityRepo(): DbEntityRepo {
        return InMemEntityRepo()
    }
}
