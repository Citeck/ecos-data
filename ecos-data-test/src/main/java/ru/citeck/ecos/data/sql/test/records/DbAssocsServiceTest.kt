package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.txn.lib.TxnContext

class DbAssocsServiceTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("assoc")
                    withType(AttributeType.ASSOC)
                    withMultiple(true)
                }
            )
        )

        val recs = (0 until 20).map {
            createRecord("id" to "rec-$it")
        }
        val rec1 = createRecord("assoc" to listOf(recs[0], recs[0]))
        assertThat(records.getAtt(rec1, "assoc[]?id").asStrList()).containsExactly(recs[0].toString())

        updateRecord(rec1, "att_add_assoc" to recs)
        assertThat(records.getAtt(rec1, "assoc[]?id").asStrList()).containsExactlyElementsOf(recs.map { it.toString() })

        recs.forEach {
            val targets = records.getAtt(it, "assoc[]?id").asStrList()
            assertThat(targets).isEmpty()
            assertThat(records.getAtt(it, "_has.assoc_src_assoc?bool").asBoolean()).isTrue()
            val sources = records.getAtt(it, "assoc_src_assoc[]?id").asStrList()
            assertThat(sources).hasSize(1)
            assertThat(sources[0]).isEqualTo(rec1.toString())
        }

        printQueryRes("SELECT * FROM ${tableRef.fullName};")
        printQueryRes("SELECT * FROM ${tableRef.withTable("ed_associations").fullName};")
        printQueryRes("SELECT * FROM ${tableRef.withTable("ed_attributes").fullName};")
    }

    @Test
    fun assocsServiceTest() {
        TxnContext.doInTxn {

            fun checkAllAssocs(vararg expectedTargets: Long) {
                val targetAssocs = assocsService.getTargetAssocs(1, "test", DbFindPage.ALL)
                assertThat(targetAssocs.entities).allMatch { it.sourceId == 1L }
                assertThat(targetAssocs.entities).allMatch { it.attribute == "test" }
                assertThat(targetAssocs.entities.map { it.targetId }).containsExactly(*expectedTargets.toTypedArray())
            }
            assocsService.createAssocs(1, "test", false, listOf(2, 3, 4), 0L)
            checkAllAssocs(2, 3, 4)
            assocsService.createAssocs(1, "test", false, listOf(2, 3, 4, 5), 0L)
            checkAllAssocs(2, 3, 4, 5)
            assocsService.createAssocs(1, "test", false, listOf(10), 0L)
            checkAllAssocs(2, 3, 4, 5, 10)
            assocsService.removeAssocs(1, "test", listOf(10, 2), false)
            checkAllAssocs(3, 4, 5)
            assocsService.removeAssocs(1, "test", listOf(10), false)
            checkAllAssocs(3, 4, 5)
        }
    }

    /**
     * A link the pre-select does not see, reached without a second transaction: it filters on
     * `__child` as well, while the unique index of the table does not, so asking for a **child**
     * link where a plain one already holds the same `(source, attribute, target)` sends an insert at
     * a row that is already there.
     *
     * That is the same collision a concurrent transaction produces - it commits its row after the
     * select and before the insert - and the outcome asserted here is the one that case needs: the
     * insert is skipped rather than raised, and the answer names only what was really created, so a
     * caller undoing its own writes or telling another application about them never names a link it
     * did not make.
     */
    @Test
    fun aLinkThatIsAlreadyThereIsSkippedRatherThanRaisedTest() {
        TxnContext.doInTxn {

            assertThat(assocsService.createAssocs(1, "test", false, listOf(2), 0L))
                .describedAs("the premise: this call is the one that creates the link")
                .containsExactly(2L)

            assertThat(assocsService.createAssocs(1, "test", true, listOf(2, 3), 0L))
                .describedAs(
                    "target 2 is already linked from this source under this attribute, so only 3 " +
                        "is created - and the call does not fail over the one that was there"
                )
                .containsExactly(3L)

            val targets = assocsService.getTargetAssocs(1, "test", DbFindPage.ALL).entities
            assertThat(targets.map { it.targetId })
                .describedAs("one row per target, the skipped one included exactly once")
                .containsExactlyInAnyOrder(2L, 3L)
            assertThat(targets.first { it.targetId == 2L }.child)
                .describedAs("and the row that was already there is left exactly as it was")
                .isFalse()
        }
    }
}
