package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.ecostype.DbEcosTypeInfo
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.role.api.records.RolesMixin
import ru.citeck.ecos.model.lib.role.dto.RoleDef
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.records2.RecordRef

class RolesMixinTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val testTypeId = "test-type"
        registerType(
            DbEcosTypeInfo(
                testTypeId, MLText(), MLText(), RecordRef.EMPTY,
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT)
                ).map { it.build() },
                emptyList()
            )
        )

        val roles = listOf(
            RoleDef.create {
                withId("testRoleId")
                withAssignees(listOf("user0", "user1"))
            }
        )

        val modelServiceFactory = object : ModelServiceFactory() {
            override fun createTypesRepo(): TypesRepo {
                return object : TypesRepo {
                    override fun getChildren(typeRef: RecordRef): List<RecordRef> {
                        return emptyList()
                    }
                    override fun getModel(typeRef: RecordRef): TypeModelDef {
                        return TypeModelDef.create {
                            withRoles(roles)
                        }
                    }
                    override fun getParent(typeRef: RecordRef): RecordRef {
                        return RecordRef.EMPTY
                    }
                }
            }
        }
        modelServiceFactory.setRecordsServices(recordsServiceFactory)

        val mixin = RolesMixin(modelServiceFactory.roleService)
        recordsDao.addAttributesMixin(mixin)

        val rec = createRecord("textAtt" to "value")

        AuthContext.runAs("user0") {
            assertThat(records.getAtt(rec, "_roles.isCurrentUserMemberOf.testRoleId?bool").asBoolean()).isTrue
        }
    }
}
