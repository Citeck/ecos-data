package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.role.api.records.RolesMixin
import ru.citeck.ecos.model.lib.role.dto.RoleDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.type.repo.TypesRepo
import ru.citeck.ecos.webapp.api.entity.EntityRef

class RolesMixinTest : DbRecordsTestBase() {

    @Test
    fun test() {

        val testTypeId = "test-type"
        registerType(
            TypeInfo.create {
                withId(testTypeId)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("textAtt")
                                    .withType(AttributeType.TEXT)
                            ).map { it.build() }
                        )
                        .build()
                )
            }
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
                    override fun getTypeInfo(typeRef: EntityRef): TypeInfo {
                        return TypeInfo.create {
                            withId(typeRef.getLocalId())
                            withModel(
                                TypeModelDef.create {
                                    withRoles(roles)
                                }
                            )
                        }
                    }
                    override fun getChildren(typeRef: EntityRef): List<EntityRef> {
                        return emptyList()
                    }
                }
            }
        }
        modelServiceFactory.setRecordsServices(recordsServiceFactory)

        val mixin = RolesMixin(modelServiceFactory.roleService)
        recordsDao.addAttributesMixin(mixin)

        AuthContext.runAs("user0") {
            val rec = createRecord("textAtt" to "value")
            assertThat(records.getAtt(rec, "_roles.isCurrentUserMemberOf.testRoleId?bool").asBoolean()).isTrue
        }
    }
}
