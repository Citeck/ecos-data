package ru.citeck.ecos.data.sql.records

import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.data.sql.PgUtils
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeRepo
import ru.citeck.ecos.data.sql.repo.DbContextManager
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef

class DbRecordsDaoTest {

    @Test
    fun test() {
        PgUtils.withDbDataSource { dataSource ->

            val testTypeId = "test-type"
            val ecosTypeRepo = object : DbEcosTypeRepo {
                override fun getTypeInfo(typeId: String): DbEcosTypeInfo? {
                    if (testTypeId == typeId) {
                        return DbEcosTypeInfo(
                            testTypeId,
                            MLText(),
                            MLText(),
                            RecordRef.EMPTY,
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT)
                                    .build(),
                                AttributeDef.create()
                                    .withId("numAtt")
                                    .withType(AttributeType.NUMBER)
                                    .build()
                            )
                        )
                    }
                    return null
                }
            }
            val contextManager = object : DbContextManager {
                override fun getCurrentTenant() = "tenant"
                override fun getCurrentUser() = "user0"
            }

            val recordsDao = DbRecordsDao(
                "test",
                DbRecordsDaoConfig(
                    DbTableRef("", "test-records-table"),
                    true,
                    true,
                    true
                ),
                ecosTypeRepo,
                dataSource,
                contextManager
            )

            //todo
        }
    }
}
