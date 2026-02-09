package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.pg.records.commons.DbRecordsTestBase
import ru.citeck.ecos.data.sql.trashcan.entity.DbTrashcanEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records3.record.dao.delete.DelStatus

class DbRecordsTrashcanTest : DbRecordsTestBase() {

    @Test
    fun testDeleteMovesToTrashcan() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val rec = AuthContext.runAs("admin") {
            createRecord("textAtt" to "hello")
        }

        val trashcanService = dataSourceCtx.getSchemaContext(tableRef.schema).trashcanService

        AuthContext.runAs("admin") {
            setAuthoritiesWithReadPerms(rec, "admin")
            val delRes = records.delete(rec)
            assertThat(delRes).isEqualTo(DelStatus.OK)
        }

        // Record should be gone from the main table
        val queryRes = records.query(baseQuery)
        assertThat(queryRes.getRecords()).isEmpty()

        // Record should be in the trashcan
        val globalRef = mainCtx.dao.getRecordsDaoCtx().getGlobalRef(rec.getLocalId())
        val refId = mainCtx.dao.getRecordsDaoCtx().recordRefService.getIdByEntityRef(globalRef)
        val trashcanEntries = trashcanService.findAllByRefId(refId)
        assertEquals(1, trashcanEntries.size)
        assertEquals(rec.getLocalId(), parseEntityData(trashcanEntries[0])["extId"])
    }

    @Test
    fun testForceDeleteDoesNotGoToTrashcan() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId("content")
                    withType(AttributeType.CONTENT)
                }
            )
        )

        val rec = AuthContext.runAs("admin") {
            createRecord("textAtt" to "to-be-force-deleted")
        }

        val trashcanService = dataSourceCtx.getSchemaContext(tableRef.schema).trashcanService

        // Force delete is used for temp files / child records
        AuthContext.runAs("admin") {
            setAuthoritiesWithReadPerms(rec, "admin")
            mainCtx.dataService.delete(
                mainCtx.dataService.findByExtId(rec.getLocalId())!!
            )
        }

        val globalRef = mainCtx.dao.getRecordsDaoCtx().getGlobalRef(rec.getLocalId())
        val refId = mainCtx.dao.getRecordsDaoCtx().recordRefService.getIdByEntityRef(globalRef)
        val trashcanEntries = trashcanService.findAllByRefId(refId)
        assertTrue(trashcanEntries.isEmpty())
    }

    @Test
    fun testDeleteAndCreateWithSameId() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val customId = "reuse-id"

        val recRef = createRecord("id" to customId, "textAtt" to "value1")
        assertThat(recRef.getLocalId()).isEqualTo(customId)

        val delRes = records.delete(recRef)
        assertThat(delRes).isEqualTo(DelStatus.OK)

        // Should be able to create a new record with the same extId
        val recRef2 = createRecord("id" to customId, "textAtt" to "value2")
        assertThat(recRef2.getLocalId()).isEqualTo(customId)
        assertThat(records.getAtt(recRef2, "textAtt").asText()).isEqualTo("value2")
    }

    @Test
    fun testTrashcanMetadata() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val rec = AuthContext.runAs("admin") {
            createRecord("textAtt" to "meta-test")
        }

        AuthContext.runAs("admin") {
            setAuthoritiesWithReadPerms(rec, "admin")
            records.delete(rec)
        }

        val trashcanService = dataSourceCtx.getSchemaContext(tableRef.schema).trashcanService
        val globalRef = mainCtx.dao.getRecordsDaoCtx().getGlobalRef(rec.getLocalId())
        val refId = mainCtx.dao.getRecordsDaoCtx().recordRefService.getIdByEntityRef(globalRef)
        val entries = trashcanService.findAllByRefId(refId)

        assertEquals(1, entries.size)
        val entry = entries[0]
        assertEquals(refId, entry.refId)
        assertEquals(tableRef.table, entry.sourceTable)

        val entityData = parseEntityData(entry)
        assertTrue(entityData.containsKey("extId"))
        assertTrue(entityData.containsKey("attributes"))
    }

    @Test
    fun testTrashcanEntryContainsCorrectMetadata() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                }
            )
        )

        val rec = AuthContext.runAs("admin") {
            createRecord("textAtt" to "metadata-test")
        }

        AuthContext.runAs("admin") {
            setAuthoritiesWithReadPerms(rec, "admin")
            records.delete(rec)
        }

        val trashcanService = dataSourceCtx.getSchemaContext(tableRef.schema).trashcanService
        val globalRef = mainCtx.dao.getRecordsDaoCtx().getGlobalRef(rec.getLocalId())
        val refId = mainCtx.dao.getRecordsDaoCtx().recordRefService.getIdByEntityRef(globalRef)

        // Verify entry metadata
        val entries = trashcanService.findAllByRefId(refId)
        assertEquals(1, entries.size)

        val entry = entries[0]
        assertEquals(refId, entry.refId)
        assertEquals(tableRef.table, entry.sourceTable)

        val entityData = parseEntityData(entry)
        assertEquals(rec.getLocalId(), entityData["extId"])

        @Suppress("UNCHECKED_CAST")
        val attributes = entityData["attributes"] as? Map<String, Any?>
        assertNotNull(attributes)
        assertEquals("metadata-test", attributes!!["textAtt"])
    }

    @Test
    fun testDeletePreservesAllEntityFields() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                },
                AttributeDef.create {
                    withId("numAtt")
                    withType(AttributeType.NUMBER)
                }
            )
        )

        val rec = AuthContext.runAs("admin") {
            createRecord(
                "textAtt" to "preserve-fields",
                "numAtt" to 42
            )
        }

        // Get original extId
        val originalExtId = rec.getLocalId()

        AuthContext.runAs("admin") {
            setAuthoritiesWithReadPerms(rec, "admin")
            records.delete(rec)
        }

        // Check trashcan entry has all fields
        val trashcanService = dataSourceCtx.getSchemaContext(tableRef.schema).trashcanService
        val globalRef = mainCtx.dao.getRecordsDaoCtx().getGlobalRef(originalExtId)
        val refId = mainCtx.dao.getRecordsDaoCtx().recordRefService.getIdByEntityRef(globalRef)
        val entries = trashcanService.findAllByRefId(refId)

        assertEquals(1, entries.size)
        val entityData = parseEntityData(entries[0])

        assertEquals(originalExtId, entityData["extId"])
        assertNotNull(entityData["created"])
        assertNotNull(entityData["modified"])
        assertNotNull(entityData["creator"])
        assertNotNull(entityData["modifier"])
        assertNotNull(entityData["attributes"])
        assertTrue(entries[0].type > 0, "type should be stored as a dedicated column")

        @Suppress("UNCHECKED_CAST")
        val attributes = entityData["attributes"] as? Map<String, Any?>
        assertNotNull(attributes)
        assertEquals("preserve-fields", attributes!!["textAtt"])
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseEntityData(entity: DbTrashcanEntity): Map<String, Any?> {
        return Json.mapper.read(entity.entityData, Map::class.java) as? Map<String, Any?> ?: emptyMap()
    }
}
