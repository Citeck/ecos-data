package ru.citeck.ecos.data.sql.pg.records

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
            val sources = records.getAtt(it, "assoc_src_assoc[]?id").asStrList()
            assertThat(sources).hasSize(1)
            assertThat(sources[0]).isEqualTo(rec1.toString())
        }

        printQueryRes("SELECT * FROM ${tableRef.fullName};")
        printQueryRes("SELECT * FROM ${tableRef.withTable("ecos_associations").fullName};")
        printQueryRes("SELECT * FROM ${tableRef.withTable("ecos_attributes").fullName};")
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
            assocsService.createAssocs(1, "test", false, listOf(2, 3, 4))
            checkAllAssocs(2, 3, 4)
            assocsService.createAssocs(1, "test", false, listOf(2, 3, 4, 5))
            checkAllAssocs(2, 3, 4, 5)
            assocsService.createAssocs(1, "test", false, listOf(10))
            checkAllAssocs(2, 3, 4, 5, 10)
            assocsService.removeAssocs(1, "test", listOf(10, 2))
            checkAllAssocs(3, 4, 5)
            assocsService.removeAssocs(1, "test", listOf(10))
            checkAllAssocs(3, 4, 5)
        }
    }
}
