package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsChildrenDeleteTest : DbRecordsTestBase() {

    companion object {
        private const val CHILD_TYPE_ID = "child_type"
        private const val CHILD_SRC_ID = "child_src_id"
    }
    private lateinit var childCtx: RecordsDaoTestCtx

    @BeforeEach
    fun beforeEach() {

        registerType(
            TypeInfo.create {
                withId(REC_TEST_TYPE_ID)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("childAssoc")
                                    .withType(AttributeType.ASSOC)
                                    .withMultiple(true)
                                    .withConfig(
                                        ObjectData.create()
                                            .set("child", true)
                                    )
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        registerType(
            TypeInfo.create {
                withId(CHILD_TYPE_ID)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("test")
                                    .build(),
                                AttributeDef.create()
                                    .withId("childChildAssoc")
                                    .withType(AttributeType.ASSOC)
                                    .withMultiple(true)
                                    .withConfig(
                                        ObjectData.create()
                                            .set("child", true)
                                    )
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        childCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("child_nodes"),
            ModelUtils.getTypeRef(CHILD_TYPE_ID),
            CHILD_SRC_ID
        )
    }

    @Test
    fun deleteWithoutPermsForChild() {

        val parentRef = createRecord()
        val childRef = childCtx.createRecord(
            "test" to "abc-value",
            "_parent" to parentRef,
            "_parentAtt" to "childAssoc"
        )

        assertExists(true, parentRef, childRef)

        setAuthoritiesWithWritePerms(parentRef, "parent-owner")
        childCtx.setAuthoritiesWithWritePerms(childRef, "child-owner")

        AuthContext.runAs("unknown") {
            assertCantDelete(parentRef)
            assertCantDelete(childRef)
        }
        AuthContext.runAs("parent-owner") {
            assertCantDelete(childRef)
            records.delete(parentRef)
            // if we have permissions to delete parent, then permissions for children shouldn't be checked
            assertExists(false, parentRef, childRef)
        }
    }

    @Test
    fun test() {

        val parentRef = createRecord()
        val children = (0 until 105).map {
            childCtx.createRecord(
                "id" to "id-$it",
                "test" to "abc-$it",
                "_parent" to parentRef,
                "_parentAtt" to "childAssoc"
            )
        }

        assertExists(true, parentRef, *children.toTypedArray())
        records.delete(parentRef)
        assertExists(false, parentRef, *children.toTypedArray())

        val parentWithChildren = records.mutate(
            listOf(
                RecordAtts(
                    EntityRef.valueOf("$RECS_DAO_ID@"),
                    ObjectData.create()
                        .set("_type", REC_TEST_TYPE_ID)
                        .set("childAssoc", listOf("child-alias-0", "child-alias-1"))
                ),
                RecordAtts(
                    EntityRef.valueOf("$CHILD_SRC_ID@"),
                    ObjectData.create()
                        .set("_type", CHILD_TYPE_ID)
                        .set("_alias", "child-alias-0")
                        .set("test", "abc")
                ),
                RecordAtts(
                    EntityRef.valueOf("$CHILD_SRC_ID@"),
                    ObjectData.create()
                        .set("_type", CHILD_TYPE_ID)
                        .set("_alias", "child-alias-1")
                        .set("childChildAssoc", listOf("child-child-alias-0"))
                ),
                RecordAtts(
                    EntityRef.valueOf("$CHILD_SRC_ID@"),
                    ObjectData.create()
                        .set("_type", CHILD_TYPE_ID)
                        .set("_alias", "child-child-alias-0")
                        .set("test", listOf("child-child-test-value"))
                )
            )
        )

        assertThat(parentWithChildren).hasSize(4)
        assertThat(
            records.getAtt(parentWithChildren[0], "childAssoc[]?id").toStrList().map {
                EntityRef.valueOf(it)
            }
        ).containsExactly(parentWithChildren[1], parentWithChildren[2])
        assertThat(
            records.getAtt(parentWithChildren[2], "childChildAssoc[]?id").toStrList().map {
                EntityRef.valueOf(it)
            }
        ).containsExactly(parentWithChildren[3])

        assertExists(true, *parentWithChildren.toTypedArray())
        records.delete(parentWithChildren[0])
        assertExists(false, *parentWithChildren.toTypedArray())
    }

    private fun assertExists(expectedExists: Boolean, vararg refs: EntityRef) {
        for (ref in refs) {
            if (expectedExists) {
                assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).describedAs(ref.toString()).isFalse
            } else {
                assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).describedAs(ref.toString()).isTrue
            }
        }
    }

    private fun assertCantDelete(ref: EntityRef) {
        assertThrows<Exception> {
            records.delete(ref)
        }
    }
}
