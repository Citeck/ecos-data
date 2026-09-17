package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.time.Duration

/**
 * Returning an attribute to its previous type gives back the data it held, at the **default**
 * [DbEcosDataProps.ColumnsProps.inPlaceAlterMaxRows].
 *
 * Every other test of the restore pins that threshold at zero, which forces every transition onto
 * the shadow-column path. That is the configuration the restore was built for, and it is not the
 * one an administrator runs: on a table below the threshold the *return* leg of a round trip is
 * very often a pair the backend can cast where it stands - `DATE -> DATETIME`, `NUMBER -> TEXT`,
 * `X -> X[]` - and an in-place `ALTER` that
 * never asks whether a backup already holds exactly what the model is asking for hands the user a
 * value derived from the lossy leg instead of the value they had.
 *
 * A matching backup wins unconditionally: there is no carve-out for pairs the backend can cast,
 * and the check comes before both the in-place and the shadow path. So the three round trips below
 * are the same statement made three times, once per shape of the reverse leg.
 *
 * Only the batch throttle is pinned here, and only so the tests do not sleep: the threshold this
 * class is about is left at its default on purpose.
 */
class DbColumnRoundTripTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            batch = DbEcosDataProps.BatchProps(batchSize = 3, batchPause = Duration.ZERO)
        )
    }

    private fun drain() {
        val schemaCtx = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schemaCtx.dataSourceCtx).drainSchemaOnce(schemaCtx)
    }

    private fun asType(type: AttributeType, multiple: Boolean = false) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(type)
                    withMultiple(multiple)
                }
            )
        )
    }

    /**
     * The textbook example, and the one an administrator hits first:
     * the forward leg loses the time of day (class C, shadow path, backup left behind) and the
     * reverse leg is a pure `timestamp AT TIME ZONE 'UTC'` cast (class S). Without the restore the
     * cast is applied to the truncated date and the instant comes back as midnight - while the
     * registry says a backup column of this very table still holds it.
     */
    @Test
    fun aDatetimeThatWentThroughDateKeepsItsTimeOfDayTest() {

        asType(AttributeType.DATETIME)
        val rec = createRecord("att" to "2024-03-05T14:37:11Z")

        asType(AttributeType.DATE)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.DATETIME)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(rec, "att").asText())
            .describedAs("a matching backup wins even when the reverse leg is a pure cast")
            .isEqualTo("2024-03-05T14:37:11Z")
    }

    /**
     * The same statement where the forward leg carries **nothing** across: `TEXT -> NUMBER` is rule
     * 17, and `hello` is not a number, so the new column stays null. The reverse leg, `NUMBER ->
     * TEXT`, is a pure `::text` cast, so it is performed in place over a column that is
     * empty, and the attribute reads empty for ever with its value sitting in the backup.
     */
    @Test
    fun aTextThatWentThroughNumberComesBackTest() {

        asType(AttributeType.TEXT)
        val rec = createRecord("att" to "hello")

        asType(AttributeType.NUMBER)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.TEXT)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(rec, "att").asText())
            .describedAs("the backup holds 'hello'; an in-place cast of the empty NUMBER column does not")
            .isEqualTo("hello")
    }

    /**
     * The multiplicity axis: `multiple -> single` is lossy and keeps the first element
     * only, `single -> multiple` is class S and is `array[x]` - a cast, and therefore in place. The
     * round trip through a scalar would otherwise hand back one element of three.
     */
    @Test
    fun anArrayThatWentThroughAScalarComesBackWholeTest() {

        asType(AttributeType.TEXT, multiple = true)
        val rec = createRecord("att" to listOf("a", "b", "c"))

        asType(AttributeType.TEXT)
        createRecord("someKey" to "unrelated")
        drain()

        asType(AttributeType.TEXT, multiple = true)
        createRecord("someKey" to "unrelated-2")
        drain()

        assertThat(records.getAtt(rec, "att[]").asStrList())
            .describedAs("all three, not the one the narrowing kept")
            .containsExactly("a", "b", "c")
    }
}
