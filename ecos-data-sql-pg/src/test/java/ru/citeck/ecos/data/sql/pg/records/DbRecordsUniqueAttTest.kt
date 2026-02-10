package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.WorkspaceScope
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbRecordsUniqueAttTest : DbRecordsTestBase() {

    companion object {
        private const val NOT_UNIQUE_ATT = "notUniqueAtt"
        private const val UNIQUE_ATT = "uniqueAtt"
        private const val UNIQUE_ATT_1 = "uniqueAtt_1"
        private const val UNIQUE_ATT_2 = "uniqueAtt_2"
        private const val MULTIPLE_UNIQUE_ATT = "multipleUniqueAtt"
    }

    @Test
    fun mainTest() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(NOT_UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT_1)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT_2)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                },
                AttributeDef.create {
                    withId(MULTIPLE_UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                    withMultiple(true)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                }
            )
        )

        createRecord(
            NOT_UNIQUE_ATT to "not unique att",
            UNIQUE_ATT to "unique att",
        )

        var ex = assertThrows<Exception> {
            createRecord(
                NOT_UNIQUE_ATT to "not unique att",
                UNIQUE_ATT to "unique att"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"unique att\"}")

        var record = createRecord(
            NOT_UNIQUE_ATT to "not unique att",
            UNIQUE_ATT to "unique att test"
        )
        var attValue = records.getAtt(record, UNIQUE_ATT).asText()
        assertThat(attValue).isEqualTo("unique att test")

        ex = assertThrows<Exception> {
            updateRecord(
                record,
                NOT_UNIQUE_ATT to "not unique att 11",
                UNIQUE_ATT to "unique att"
            )
        }
        assertThat(ex.message).contains("$record has non-unique attributes {\"uniqueAtt\":\"unique att\"}")

        val updatedRecord = updateRecord(
            record,
            NOT_UNIQUE_ATT to "not unique att 11",
            UNIQUE_ATT to "another unique text"
        )

        attValue = records.getAtt(updatedRecord, UNIQUE_ATT).asText()
        assertThat(attValue).isEqualTo("another unique text")

        createRecord(MULTIPLE_UNIQUE_ATT to listOf("text1", "text2"))
        record = createRecord(MULTIPLE_UNIQUE_ATT to listOf("text1", "text2"))

        val attListValue = records.getAtt(record, "$MULTIPLE_UNIQUE_ATT[]?str").asStrList()
        assertThat(attListValue).isEqualTo(listOf("text1", "text2"))

        record = createRecord(
            UNIQUE_ATT to "value 1",
            UNIQUE_ATT_1 to "value 1",
            UNIQUE_ATT_2 to "value 1"
        )

        ex = assertThrows<Exception> {
            createRecord(
                UNIQUE_ATT to "value 1",
                UNIQUE_ATT_1 to "value 2",
                UNIQUE_ATT_2 to "value 2"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"value 1\"}")

        ex = assertThrows<Exception> {
            createRecord(
                UNIQUE_ATT to "value 1",
                UNIQUE_ATT_1 to "value 1",
                UNIQUE_ATT_2 to "value 2"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"value 1\",\"uniqueAtt_1\":\"value 1\"}")
    }

    @Test
    fun `uniqueAtt with private workspace scope test`() {
        val privateWSTypeDao = registerType()
            .withAttributes(
                AttributeDef.create {
                    withId(NOT_UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create()
                            .set("unique", true)
                            .set("uniqueScope", "WORKSPACE")
                    )
                }
            )
            .withWorkspaceScope(WorkspaceScope.PRIVATE).register()

        val ws0 = "workspace-0"
        val ws1 = "workspace-1"

        var record0 = privateWSTypeDao.createRecord(
            RecordConstants.ATT_WORKSPACE to ws0,
            NOT_UNIQUE_ATT to "value",
            UNIQUE_ATT to "unique value"
        )
        assertWs(record0, ws0)

        val record1 = privateWSTypeDao.createRecord(
            RecordConstants.ATT_WORKSPACE to ws1,
            NOT_UNIQUE_ATT to "value",
            UNIQUE_ATT to "unique value"
        )
        assertWs(record1, ws1)

        var ex = assertThrows<Exception> {
            privateWSTypeDao.createRecord(
                RecordConstants.ATT_WORKSPACE to ws0,
                NOT_UNIQUE_ATT to "value",
                UNIQUE_ATT to "unique value"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"unique value\"}")

        ex = assertThrows<Exception> {
            privateWSTypeDao.createRecord(
                RecordConstants.ATT_WORKSPACE to ws1,
                NOT_UNIQUE_ATT to "value",
                UNIQUE_ATT to "unique value"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"unique value\"}")
    }

    @Test
    fun `uniqueAtt with public workspace scope test`() {
        val publicWSTypeDao = registerType()
            .withAttributes(
                AttributeDef.create {
                    withId(NOT_UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId(UNIQUE_ATT)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create().set("unique", true)
                    )
                }
            )
            .withWorkspaceScope(WorkspaceScope.PUBLIC).register()

        val ws0 = "workspace-0"

        val record0 = publicWSTypeDao.createRecord(
            RecordConstants.ATT_WORKSPACE to ws0,
            NOT_UNIQUE_ATT to "value",
            UNIQUE_ATT to "unique value"
        )
        assertWs(record0, "")

        val ex = assertThrows<Exception> {
            publicWSTypeDao.createRecord(
                RecordConstants.ATT_WORKSPACE to ws0,
                NOT_UNIQUE_ATT to "value",
                UNIQUE_ATT to "unique value"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes {\"uniqueAtt\":\"unique value\"}")
    }

    @Test
    fun `uniqueAtt with globalCheck in private workspace test`() {
        val localUniqueAtt = "localUniqueAtt"
        val globalUniqueAtt = "globalUniqueAtt"

        val privateWSTypeDao = registerType()
            .withAttributes(
                AttributeDef.create {
                    withId(localUniqueAtt)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create()
                            .set("unique", true)
                            .set("uniqueScope", "WORKSPACE")
                    )
                },
                AttributeDef.create {
                    withId(globalUniqueAtt)
                    withType(AttributeType.TEXT)
                    withConfig(
                        ObjectData.create()
                            .set("unique", true)
                            .set("uniqueScope", "GLOBAL")
                    )
                }
            )
            .withWorkspaceScope(WorkspaceScope.PRIVATE)
            .register()

        val ws0 = "workspace-0"
        val ws1 = "workspace-1"

        privateWSTypeDao.createRecord(
            RecordConstants.ATT_WORKSPACE to ws0,
            localUniqueAtt to "local-value",
            globalUniqueAtt to "global-value"
        )

        privateWSTypeDao.createRecord(
            RecordConstants.ATT_WORKSPACE to ws1,
            localUniqueAtt to "local-value",
            globalUniqueAtt to "different-global"
        )

        val ex = assertThrows<Exception> {
            privateWSTypeDao.createRecord(
                RecordConstants.ATT_WORKSPACE to ws1,
                localUniqueAtt to "another-local",
                globalUniqueAtt to "global-value"
            )
        }
        assertThat(ex.message).contains("has non-unique attributes")
        assertThat(ex.message).contains("globalUniqueAtt")
    }

    private fun assertWs(rec: EntityRef, expectedWs: String) {
        assertThat(records.getAtt(rec, "${RecordConstants.ATT_WORKSPACE}?localId").asText()).isEqualTo(expectedWs)
    }
}
