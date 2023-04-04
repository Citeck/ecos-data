package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
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

    @Test
    fun test() {

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

        val childCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("child_nodes"),
            ModelUtils.getTypeRef(CHILD_TYPE_ID),
            CHILD_SRC_ID
        )

        val parentRef = createRecord()
        val children = (0 until 10).map {
            childCtx.createRecord(
                "test" to "abc-$it",
                "_parent" to parentRef,
                "_parentAtt" to "childAssoc"
            )
        }

        fun assertExists(expectedExists: Boolean, vararg refs: EntityRef) {
            for (ref in refs) {
                if (expectedExists) {
                    assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).describedAs(ref.toString()).isFalse
                } else {
                    assertThat(records.getAtt(ref, "_notExists?bool").asBoolean()).describedAs(ref.toString()).isTrue
                }
            }
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
}
