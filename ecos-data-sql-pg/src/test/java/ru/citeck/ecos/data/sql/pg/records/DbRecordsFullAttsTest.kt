package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.YamlUtils
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.ContentUtils
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsFullAttsTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerType()
            .withAttributes(
                AttributeDef.create().withId("text0"),
                AttributeDef.create().withId("number").withType(AttributeType.NUMBER)
            ).withWorkspaceScope(WorkspaceScope.PRIVATE).register()

        registerType()
            .asSubTypeWithId("sub-type")
            .withAttributes(AttributeDef.create().withId("text0"))
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .register()

        val rec0 = createRecord(
            "id" to "id-0",
            "text0" to "value-0",
            "number" to 123,
            "_workspace" to "test"
        )
        workspaceService.setUserWorkspaces("user", setOf("test"))

        assertThat(rec0.getLocalId()).isEqualTo("id-0")

        fun assertRecAtts(text: String, number: Double?) {
            val atts = records.getAtts(rec0, listOf("text0", "number?num"))
            assertThat(atts["text0"]).isEqualTo(DataValue.createStr(text))
            assertThat(atts["number?num"]).isEqualTo(DataValue.of(number))
        }
        assertRecAtts("value-0", 123.0)

        fun mutateContent(contentAtt: String, content: DataValue, vararg additionalAtts: Pair<String, Any?>): EntityRef {
            val atts = RecordAtts(rec0.withoutLocalId())
            atts[contentAtt] = ContentUtils.createContentObjFromText(
                YamlUtils.toString(content),
                fileName = "content.yaml",
                mimeType = "application/x-yaml"
            )
            additionalAtts.forEach { (k, v) ->
                atts[k] = v
            }
            return records.mutate(atts)
        }

        runAsUser {
            mutateContent(
                DbRecordsControlAtts.FULL_ATTS,
                DataValue.createObj().set("id", "id-0").set("text0", "new-value")
            )
        }

        assertRecAtts("new-value", null)

        mutateContent(
            RecordConstants.ATT_SELF,
            DataValue.createObj().set("id", "id-0")
                .set("text0", "new-value222")
                .set("number", 543)
        )
        assertRecAtts("new-value222", 543.0)
        runAsUser {
            val ex = assertThrows<Exception> {
                mutateContent(
                    RecordConstants.ATT_SELF,
                    DataValue.createObj().set("id", "id-0")
                        .set("text0", "new-value333")
                        .set("number", 732)
                )
            }
            assertThat(ex).hasMessage("Record 'test-app/test@id-0' already exists. The id must be unique.")
        }
        assertRecAtts("new-value222", 543.0)
        records.mutate(rec0, mapOf("text0" to "new-val333"))
        assertRecAtts("new-val333", 543.0)
        records.mutate(rec0, mapOf("text0" to "new-val444", DbRecordsControlAtts.FULL_ATTS to true))
        assertRecAtts("new-val444", null)

        listOf(RecordConstants.ATT_SELF, DbRecordsControlAtts.FULL_ATTS).forEach { attWithContent ->
            runAsUser {
                val id = "new_entity_by_att_$attWithContent"
                val textAttValue = "val0-$attWithContent"
                val newRef = mutateContent(
                    contentAtt = attWithContent,
                    content = DataValue.createObj().set("id", id)
                        .set("text0", textAttValue),
                    "_workspace" to "test",
                    "_type" to "sub-type"
                )
                assertThat(newRef.getLocalId()).isEqualTo(id)

                fun assertAtt(att: String, expected: Any) {
                    assertThat(records.getAtt(newRef, att)).isEqualTo(DataValue.of(expected))
                }
                println("attWithContent: $attWithContent")
                assertAtt("_workspace?localId", "test")
                assertAtt("_type?localId", "sub-type")
                assertAtt("text0", textAttValue)
            }
        }
    }

    private fun <T> runAsUser(action: () -> T):T {
        return AuthContext.runAs("user", listOf("GROUP_EVERYONE"), action)
    }
}


