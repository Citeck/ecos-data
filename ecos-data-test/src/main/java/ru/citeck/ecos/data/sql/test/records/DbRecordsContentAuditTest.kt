package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.data.sql.records.DbRecordsControlAtts
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Instant
import java.util.Base64

class DbRecordsContentAuditTest : DbRecordsTestBase() {

    companion object {
        private const val CONTENT = "content"
        private const val TEXT = "text"

        private val CONTENT_CREATED = Instant.parse("2017-12-07T07:14:54Z")
        private const val CONTENT_CREATOR = "content-creator"
    }

    @BeforeEach
    fun beforeEach() {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId(CONTENT)
                    withType(AttributeType.CONTENT)
                },
                AttributeDef.create {
                    withId(TEXT)
                }
            )
        )
    }

    @Test
    fun contentCopiedFromTempFileGetsPassedCreatedAndCreator() {
        val tempFile = createTempRecord(content = "abc".toByteArray())

        val ref = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
            DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
            CONTENT to tempFile
        )

        assertThat(getContentCreated(ref)).isEqualTo(CONTENT_CREATED)
        assertThat(getContentCreator(ref)).isEqualTo(CONTENT_CREATOR)
        assertThat(records.getAtt(ref, "$CONTENT.bytes").asText())
            .isEqualTo(Base64.getEncoder().encodeToString("abc".toByteArray()))
    }

    @Test
    fun contentCopiedFromAnotherRecordGetsPassedCreatedAndCreator() {
        val source = createRecord(CONTENT to dataUrlContent("abc"))
        val sourceCreated = getContentCreated(source)

        val ref = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
            DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
            CONTENT to source
        )

        assertThat(getContentCreated(ref)).isEqualTo(CONTENT_CREATED)
        assertThat(getContentCreator(ref)).isEqualTo(CONTENT_CREATOR)
        assertThat(getContentCreated(source)).isEqualTo(sourceCreated)
    }

    @Test
    fun contentGivenAsArrayGetsPassedCreatedAndCreator() {
        val ref = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
            DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
            CONTENT to listOf(dataUrlContent("abc"))
        )

        assertThat(getContentCreated(ref)).isEqualTo(CONTENT_CREATED)
        assertThat(getContentCreator(ref)).isEqualTo(CONTENT_CREATOR)
    }

    @Test
    fun uploadedContentGetsPassedCreatedAndCreatorFromPersonRef() {
        val ref = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
            DbRecordsControlAtts.CONTENT_CREATOR to "emodel/person@$CONTENT_CREATOR",
            CONTENT to dataUrlContent("abc")
        )

        assertThat(getContentCreated(ref)).isEqualTo(CONTENT_CREATED)
        assertThat(getContentCreator(ref)).isEqualTo(CONTENT_CREATOR)
    }

    @Test
    fun creatorWithAtSignIsPassedAsPersonRef() {
        val ref = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATOR to "emodel/person@user@example.com",
            CONTENT to dataUrlContent("abc")
        )

        assertThat(getContentCreator(ref)).isEqualTo("user@example.com")
    }

    @Test
    fun contentWithoutControlAttsIsCreatedNowByCurrentUser() {
        val beforeCreation = Instant.now()

        val ref = AuthContext.runAs("admin") {
            createRecord(CONTENT to dataUrlContent("abc"))
        }

        assertThat(getContentCreated(ref)).isAfterOrEqualTo(beforeCreation)
        assertThat(getContentCreator(ref)).isEqualTo("admin")
    }

    @Test
    fun onlyCreatedOrOnlyCreatorMayBePassed() {
        val beforeCreation = Instant.now()

        val withCreated = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
            CONTENT to dataUrlContent("abc")
        )
        assertThat(getContentCreated(withCreated)).isEqualTo(CONTENT_CREATED)
        assertThat(getContentCreator(withCreated)).isEqualTo(AuthContext.getCurrentUser())

        val withCreator = createRecord(
            DbRecordsControlAtts.DISABLE_AUDIT to true,
            DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
            CONTENT to dataUrlContent("abc")
        )
        assertThat(getContentCreated(withCreator)).isAfterOrEqualTo(beforeCreation)
        assertThat(getContentCreator(withCreator)).isEqualTo(CONTENT_CREATOR)
    }

    @Test
    fun createdWithoutDisabledAuditIsRejected() {
        val error = assertThrows<RuntimeException> {
            createRecord(
                DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
                CONTENT to dataUrlContent("abc")
            )
        }
        assertThat(error.message).contains(DbRecordsControlAtts.DISABLE_AUDIT)
    }

    @Test
    fun creatorWithoutDisabledAuditIsRejected() {
        val error = assertThrows<RuntimeException> {
            createRecord(
                DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
                CONTENT to dataUrlContent("abc")
            )
        }
        assertThat(error.message).contains(DbRecordsControlAtts.DISABLE_AUDIT)
    }

    @Test
    fun invalidCreatedIsRejected() {
        val error = assertThrows<RuntimeException> {
            createRecord(
                DbRecordsControlAtts.DISABLE_AUDIT to true,
                DbRecordsControlAtts.CONTENT_CREATED to "not-a-date",
                CONTENT to dataUrlContent("abc")
            )
        }
        assertThat(error.message).contains(DbRecordsControlAtts.CONTENT_CREATED)
    }

    @Test
    fun mutationWithoutContentKeepsWrittenContent() {
        val ref = createRecord(CONTENT to dataUrlContent("abc"))
        val createdBefore = getContentCreated(ref)
        val creatorBefore = getContentCreator(ref)
        assertThat(createdBefore).isAfter(CONTENT_CREATED)
        assertThat(creatorBefore).isNotBlank().isNotEqualTo(CONTENT_CREATOR)

        records.mutate(
            ref,
            mapOf(
                DbRecordsControlAtts.DISABLE_AUDIT to true,
                DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
                DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
                TEXT to "changed"
            )
        )

        assertThat(records.getAtt(ref, TEXT).asText()).isEqualTo("changed")
        assertThat(getContentCreated(ref)).isEqualTo(createdBefore)
        assertThat(getContentCreator(ref)).isEqualTo(creatorBefore)
    }

    @Test
    fun newContentOfExistingRecordGetsPassedCreatedAndCreator() {
        val ref = createRecord(CONTENT to dataUrlContent("abc"))

        records.mutate(
            ref,
            mapOf(
                DbRecordsControlAtts.DISABLE_AUDIT to true,
                DbRecordsControlAtts.CONTENT_CREATED to CONTENT_CREATED,
                DbRecordsControlAtts.CONTENT_CREATOR to CONTENT_CREATOR,
                CONTENT to dataUrlContent("def")
            )
        )

        assertThat(getContentCreated(ref)).isEqualTo(CONTENT_CREATED)
        assertThat(getContentCreator(ref)).isEqualTo(CONTENT_CREATOR)
    }

    private fun dataUrlContent(text: String): Map<String, Any> {
        val base64 = Base64.getEncoder().encodeToString(text.toByteArray())
        return mapOf(
            "url" to "data:text/plain;base64,$base64",
            "originalName" to "$text.txt"
        )
    }

    private fun getContentCreated(ref: EntityRef): Instant {
        return records.getAtt(ref, "$CONTENT.created").getAsInstantOrEpoch()
    }

    private fun getContentCreator(ref: EntityRef): String {
        return records.getAtt(ref, "$CONTENT.creator").asText()
    }
}
