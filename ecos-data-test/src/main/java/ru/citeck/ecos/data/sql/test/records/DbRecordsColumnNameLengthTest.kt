package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.exception.I18nRuntimeException
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef

/**
 * A column name longer than the backend identifier limit is rejected before any DDL runs.
 * PostgreSQL would otherwise silently truncate it: the read path then looks the column up by the
 * full name and finds nothing, and every later write fails on "column already exists".
 */
class DbRecordsColumnNameLengthTest : DbRecordsTestBase() {

    companion object {
        private const val MESSAGE_KEY = "ecos-data.column-name-too-long"
        private const val SHORT_ATT = "shortAtt"
    }

    private fun limit(): Int = dbSchemaDao.getMaxColumnNameBytes()

    private fun attId(length: Int): String = "a".repeat(length)

    private fun i18nCause(ex: Throwable): I18nRuntimeException {
        var cause: Throwable? = ex
        while (cause != null) {
            if (cause is I18nRuntimeException) {
                return cause
            }
            cause = cause.cause
        }
        error("I18nRuntimeException is not found in the cause chain of $ex")
    }

    @Test
    fun backendLimitMirrorsPostgresTest() {
        // every backend, the in-memory one included, reports the portable platform limit:
        // application tests must reject the same models that production PostgreSQL rejects
        assertThat(limit()).isEqualTo(63)
    }

    @Test
    fun maxLengthAttIdIsAllowedTest() {

        val id = attId(limit())
        registerAtts(listOf(AttributeDef.create { withId(id) }))

        val rec = createRecord(id to "value")

        assertThat(records.getAtt(rec, id).asText()).isEqualTo("value")
        assertThat(getColumns().map { it.name }).contains(id)
    }

    @Test
    fun tooLongAttIdFailsFastOnFirstWriteTest() {

        val id = attId(limit() + 1)
        registerAtts(listOf(AttributeDef.create { withId(id) }))

        val ex = assertThrows<Exception> {
            createRecord(id to "value")
        }
        assertThat(i18nCause(ex).messageKey).isEqualTo(MESSAGE_KEY)
        assertThat(ex.message).contains(id).contains("too long").contains(limit().toString())

        assertThat(records.query(baseQuery).getRecords()).isEmpty()
        // the guard runs before CREATE TABLE, so nothing is left behind
        assertThat(getColumns()).isEmpty()
    }

    @Test
    fun tooLongAttIdAddedToExistingTypeFailsEveryWriteButReadsWorkTest() {

        registerAtts(listOf(AttributeDef.create { withId(SHORT_ATT) }))
        val rec0 = createRecord(SHORT_ATT to "v0")

        val longId = attId(limit() + 1)
        registerAtts(
            listOf(
                AttributeDef.create { withId(SHORT_ATT) },
                AttributeDef.create { withId(longId) }
            )
        )

        // the long attribute has no value here, the guard still fires: the schema sync
        // would otherwise try to add the column on this very write
        val ex = assertThrows<Exception> {
            updateRecord(rec0, SHORT_ATT to "v1")
        }
        assertThat(i18nCause(ex).messageKey).isEqualTo(MESSAGE_KEY)

        assertThat(records.getAtt(rec0, SHORT_ATT).asText()).isEqualTo("v0")
    }

    @Test
    fun tooLongAttIdInMigrationPreviewTest() {

        val id = attId(limit() + 1)
        registerAtts(listOf(AttributeDef.create { withId(id) }))

        val ex = assertThrows<Exception> {
            recordsDao.runMigrations(REC_TEST_TYPE_REF, mock = true)
        }
        assertThat(i18nCause(ex).messageKey).isEqualTo(MESSAGE_KEY)
        assertThat(ex.message).contains(id)
    }

    @Test
    fun tooLongAspectAttIdFailsFastTest() {

        val aspectId = "long-aspect"
        val prefix = "$aspectId:"
        val aspectAttId = prefix + attId(limit() + 1 - prefix.length)
        assertThat(aspectAttId.toByteArray(Charsets.UTF_8)).hasSize(limit() + 1)

        registerAtts(listOf(AttributeDef.create { withId(SHORT_ATT) }))
        // the aspect is not declared on the type: writing one of its attributes attaches it to the
        // record, and its columns then come through getColumnsForAspects instead of the type model
        registerAspect(
            AspectInfo.create {
                withId(aspectId)
                withAttributes(listOf(AttributeDef.create { withId(aspectAttId) }))
            }
        )

        val ex = assertThrows<Exception> {
            createRecord(aspectAttId to "value")
        }
        assertThat(i18nCause(ex).messageKey).isEqualTo(MESSAGE_KEY)
        assertThat(ex.message).contains(aspectAttId)
    }
}
