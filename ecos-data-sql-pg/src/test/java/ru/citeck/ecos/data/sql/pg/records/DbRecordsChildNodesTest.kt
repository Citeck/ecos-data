package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.records3.record.atts.dto.RecordAtts
import ru.citeck.ecos.records3.record.request.RequestContext

class DbRecordsChildNodesTest : DbRecordsTestBase() {

    @Test
    fun createWithChildrenTest() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("strField")
                },
                AttributeDef.create {
                    withId("childAssoc")
                    withType(AttributeType.ASSOC)
                    withConfig(ObjectData.create("""{"child":true}"""))
                }
            )
        )

        val mainRec = RecordAtts(RecordRef.create(recordsDao.getId(), ""))
        mainRec.setAtt("strField", "parent-test")
        mainRec.setAtt("childAssoc?assoc", "alias-123")
        mainRec.setAtt("_type", REC_TEST_TYPE_REF)

        val childRec = RecordAtts(RecordRef.create(recordsDao.getId(), ""))
        childRec.setAtt("_alias", "alias-123")
        childRec.setAtt("strField", "child-test")
        childRec.setAtt("_type", REC_TEST_TYPE_REF)

        val mutatedRecords = records.mutate(listOf(mainRec, childRec))
        assertThat(mutatedRecords).hasSize(2)

        val childAssocs = records.getAtt(mutatedRecords[0], "childAssoc[]?id").asList(RecordRef::class.java)
        assertThat(childAssocs).containsExactly(mutatedRecords[1])

        val parentId = records.getAtt(mutatedRecords[1], "_parent?id").getAs(RecordRef::class.java)
        assertThat(parentId).isEqualTo(mutatedRecords[0])

        // test with sourceId mapping

        val otherSourceId = "other-source-id"
        val mutatedRecords2 = RequestContext.doWithCtx({ ctxData ->
            ctxData.withSourceIdMapping(mapOf(recordsDao.getId() to otherSourceId))
        }) {
            records.mutate(listOf(mainRec, childRec))
        }
        val parentId2 = records.getAtt(mutatedRecords2[1], "_parent?id").getAs(RecordRef::class.java)
        assertThat(parentId2?.sourceId).isEqualTo(otherSourceId)
    }
}
