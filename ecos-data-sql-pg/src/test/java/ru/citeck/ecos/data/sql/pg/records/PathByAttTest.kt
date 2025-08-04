package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class PathByAttTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerType()
            .withAttributes(
                AttributeDef.create()
                    .withId("name"),
                AttributeDef.create()
                    .withId("targetAssoc")
                    .withType(AttributeType.ASSOC),
                AttributeDef.create()
                    .withId("childAssoc")
                    .withType(AttributeType.ASSOC)
                    .withConfig(ObjectData.create().set("child", true))
            ).register()

        val root = createRecord("name" to "ROOT")
        val child1 = createRecord(
            "name" to "child-1",
            "_parent" to root,
            "_parentAtt" to "childAssoc",
            "targetAssoc" to root
        )
        val child2 = createRecord(
            "name" to "child-2",
            "_parent" to child1,
            "_parentAtt" to "childAssoc",
            "targetAssoc" to child1
        )
        val child3 = createRecord(
            "name" to "child-3",
            "_parent" to child2,
            "_parentAtt" to "childAssoc",
            "targetAssoc" to child2
        )

        listOf("_pathByParent", "_pathByAssoc._parent").forEach { baseAttWithPath ->

            val pathByParent = records.getAtt(child3, "$baseAttWithPath[]{displayName:?disp,ref:?id}")

            assertThat(pathByParent).isEqualTo(
                DataValue.createArr()
                    .add(
                        DataValue.createObj()
                            .set("displayName", "ROOT")
                            .set("ref", root.toString())
                    )
                    .add(
                        DataValue.createObj()
                            .set("displayName", "child-1")
                            .set("ref", child1.toString())
                    )
                    .add(
                        DataValue.createObj()
                            .set("displayName", "child-2")
                            .set("ref", child2.toString())
                    )
                    .add(
                        DataValue.createObj()
                            .set("displayName", "child-3")
                            .set("ref", child3.toString())
                    )
            )
        }

        val pathByTargetAssoc = records.getAtt(root, "_pathByAssoc.targetAssoc[]{displayName:?disp,ref:?id}")

        assertThat(pathByTargetAssoc).isEqualTo(
            DataValue.createArr()
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-3")
                        .set("ref", child3.toString())
                )
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-2")
                        .set("ref", child2.toString())
                )
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-1")
                        .set("ref", child1.toString())
                )
                .add(
                    DataValue.createObj()
                        .set("displayName", "ROOT")
                        .set("ref", root.toString())
                )
        )

        val singleValuePath = records.getAtt(child3, "_pathByAssoc.targetAssoc[]{displayName:?disp,ref:?id}")
        assertThat(singleValuePath).isEqualTo(
            DataValue.createArr()
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-3")
                        .set("ref", child3.toString())
                )
        )

        val pathWithoutCurrentRecord = records.getAtt(
            root,
            "assoc_src_targetAssoc._pathByAssoc.targetAssoc[]{displayName:?disp,ref:?id}"
        )

        assertThat(pathWithoutCurrentRecord).isEqualTo(
            DataValue.createArr()
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-3")
                        .set("ref", child3.toString())
                )
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-2")
                        .set("ref", child2.toString())
                )
                .add(
                    DataValue.createObj()
                        .set("displayName", "child-1")
                        .set("ref", child1.toString())
                )
        )
    }
}
