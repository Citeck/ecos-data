package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.type.dto.TypeModelDef
import ru.citeck.ecos.model.lib.utils.ModelUtils

/**
 * Two types whose records share one table must not fight over one column. The freeze is the only
 * outcome that keeps both types' data intact, so it is asserted on the data, not on the log.
 */
class DbColumnTypeConflictTest : DbRecordsTestBase() {

    private fun registerChildInSameTable(attType: AttributeType) {
        registerType(
            TypeInfo.create {
                withId("child-type")
                withParentRef(REC_TEST_TYPE_REF)
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("sharedAtt")
                                    .withType(attType)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )
    }

    @Test
    fun conflictingDeclarationsFreezeTheColumnTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("sharedAtt")
                    withType(AttributeType.NUMBER)
                },
                AttributeDef.create { withId("otherAtt") }
            )
        )
        val parentRec = createRecord("sharedAtt" to 42, "otherAtt" to "parent")
        assertThat(getColumns().first { it.name == "sharedAtt" }.type).isEqualTo(DbColumnType.DOUBLE)

        // the child declares the same attribute id as text. DOUBLE -> TEXT is a supported
        // conversion, so without the freeze the next mutation of a child record would convert the
        // column and the parent's numeric records would follow it.
        registerChildInSameTable(AttributeType.TEXT)

        // the child record sets nothing - the expected column set still comes from the child's
        // model, so the migration decision is reached either way
        val childRec = records.create(RECS_DAO_ID, mapOf("_type" to "child-type"))

        assertThat(getColumns().first { it.name == "sharedAtt" }.type)
            .describedAs("the column must not move while two types disagree about it")
            .isEqualTo(DbColumnType.DOUBLE)

        // both records are alive and readable
        assertThat(records.getAtt(parentRec, "sharedAtt").asDouble()).isEqualTo(42.0)
        assertThat(records.getAtt(childRec, "_type?id").asText()).endsWith("child-type")

        // and the registry still describes what the column holds
        val meta = getTableCtx().getSchemaCtx().columnMetaService
            .getByTable(tableRef.table)
            .first { it.columnName == "sharedAtt" }
        assertThat(meta.attType).isEqualTo(DbColumnSemanticType.Model(AttributeType.NUMBER))
    }

    @Test
    fun agreeingDeclarationsDoNotFreezeTheColumnTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("sharedAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("sharedAtt" to 42)
        registerChildInSameTable(AttributeType.NUMBER)

        // the parent moves the attribute to text and the child agrees, so nothing is frozen
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("sharedAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        registerChildInSameTable(AttributeType.TEXT)
        createRecord("sharedAtt" to "text-now")

        assertThat(getColumns().first { it.name == "sharedAtt" }.type).isEqualTo(DbColumnType.TEXT)
    }

    @Test
    fun aChildWithItsOwnTableIsNotAConflictTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("sharedAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        createRecord("sharedAtt" to 42)

        // same attribute id, a type the parent never adopts, but this child stores its records
        // elsewhere, so the two declarations never meet in one column. BOOLEAN (not TEXT, which the
        // parent moves to next) keeps the two declarations genuinely disagreeing at the moment of
        // the mutation below - otherwise the filter under test is not actually exercised.
        registerType()
            .asSubTypeWithId("own-table-child")
            .withSourceId("own-table-child-src")
            .withAttributes(
                AttributeDef.create()
                    .withId("sharedAtt")
                    .withType(AttributeType.BOOLEAN)
            )
            .register()

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("sharedAtt")
                    withType(AttributeType.TEXT)
                }
            )
        )
        createRecord("sharedAtt" to "text-now")

        assertThat(getColumns().first { it.name == "sharedAtt" }.type)
            .describedAs("a type that does not share the table must not freeze it")
            .isEqualTo(DbColumnType.TEXT)
    }

    @Test
    fun conflictAcrossThreeLevelsFreezesTheColumnTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("sharedAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        val rootRec = createRecord("sharedAtt" to 42)
        assertThat(getColumns().first { it.name == "sharedAtt" }.type).isEqualTo(DbColumnType.DOUBLE)

        // middle of the chain - declares nothing itself, it only matters as the hop the
        // storage-boundary walk has to climb through to reach the root
        registerType(
            TypeInfo.create {
                withId("child-type")
                withParentRef(REC_TEST_TYPE_REF)
            }
        )

        // two levels below the root, and disagrees with the root's declaration. If the walk only
        // climbed one level (to child-type) instead of all the way to the root, it would never see
        // the root's NUMBER declaration and this would convert the column instead of freezing it.
        registerType(
            TypeInfo.create {
                withId("grandchild-type")
                withParentRef(ModelUtils.getTypeRef("child-type"))
                withModel(
                    TypeModelDef.create()
                        .withAttributes(
                            listOf(
                                AttributeDef.create()
                                    .withId("sharedAtt")
                                    .withType(AttributeType.TEXT)
                                    .build()
                            )
                        )
                        .build()
                )
            }
        )

        val grandchildRec = records.create(RECS_DAO_ID, mapOf("_type" to "grandchild-type"))

        assertThat(getColumns().first { it.name == "sharedAtt" }.type)
            .describedAs("the walk must climb all the way to the storage root, not stop one level up")
            .isEqualTo(DbColumnType.DOUBLE)

        assertThat(records.getAtt(rootRec, "sharedAtt").asDouble()).isEqualTo(42.0)
        assertThat(records.getAtt(grandchildRec, "_type?id").asText()).endsWith("grandchild-type")
    }
}
