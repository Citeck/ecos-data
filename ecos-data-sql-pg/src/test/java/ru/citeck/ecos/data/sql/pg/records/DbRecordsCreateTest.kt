package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.data.MLText
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.commons.mime.MimeTypes
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.i18n.I18nContext
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy
import ru.citeck.ecos.records2.RecordConstants
import ru.citeck.ecos.records2.RecordRef
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class DbRecordsCreateTest : DbRecordsTestBase() {

    @Test
    fun test() {

        registerAtts(
            listOf(
                AttributeDef.create { withId("textAtt") }
            )
        )

        val ref = RecordRef.create(recordsDao.getId(), "unknown-id")
        val exception = assertThrows<Exception> {
            val data = ObjectData.create()
            data[RecordConstants.ATT_TYPE] = REC_TEST_TYPE_REF
            records.mutate(ref, data)
        }
        assertThat(exception.message).contains("doesn't found")
    }

    @Test
    fun copyTest() {

        registerAtts(
            AttributeType.values().map { type ->
                AttributeDef.create {
                    withId(type.toString().lowercase())
                    withType(type)
                }
            }
        )
        setPermsPolicy(TypePermsPolicy.OWN)

        val bytes = ByteArray(10) { it.toByte() }

        val srcAtts = ObjectData.create()
            .set("assoc", "some/ref@abc")
            .set("person", "some/ref@person")
            .set("authority_group", "some/ref@auth-group")
            .set("authority", "some/ref@authority")
            .set("text", "text")
            .set("mltext", MLText(I18nContext.RUSSIAN to "abc", I18nContext.ENGLISH to "def"))
            .set("number", 123)
            .set("boolean", true)
            .set("date", Instant.now().truncatedTo(ChronoUnit.DAYS))
            .set("datetime", Instant.now().plus(Duration.ofDays(10)))
            .set(
                "content",
                DataValue.createObj()
                    .setStr("url", "data:${MimeTypes.APP_BIN};base64,${Base64.getEncoder().encodeToString(bytes)}")
            )
            .set("json", DataValue.createObj().set("abc", "def"))
            .set("binary", Base64.getEncoder().encodeToString(bytes))

        fun checkAtts(ref: RecordRef, expectedTextAtt: String) {
            val atts = records.getAtts(
                ref,
                mapOf(
                    "assoc" to "assoc?id",
                    "person" to "person?id",
                    "authority_group" to "authority_group?id",
                    "authority" to "authority?id",
                    "text" to "text",
                    "mltext" to "mltext?json",
                    "number" to "number?num",
                    "boolean" to "boolean?bool",
                    "date" to "date",
                    "datetime" to "datetime",
                    "content" to "content.bytes",
                    "json" to "json?json",
                    "binary" to "binary"
                )
            ).getAtts()

            assertThat(atts["assoc"].asText()).isEqualTo("some/ref@abc")
            assertThat(atts["person"].asText()).isEqualTo("some/ref@person")
            assertThat(atts["authority_group"].asText()).isEqualTo("some/ref@auth-group")
            assertThat(atts["authority"].asText()).isEqualTo("some/ref@authority")
            assertThat(atts["text"].asText()).isEqualTo(expectedTextAtt)
            assertThat(atts["mltext"].getAsNotNull(MLText::class.java)).isEqualTo(
                MLText(I18nContext.RUSSIAN to "abc", I18nContext.ENGLISH to "def")
            )
            assertThat(atts["number"].asInt()).isEqualTo(123)
            assertThat(atts["boolean"].asBoolean()).isTrue
            assertThat(atts["date"].getAsInstant()).isEqualTo(srcAtts["date"].getAsInstant())
            assertThat(atts["datetime"].getAsInstant()).isEqualTo(srcAtts["datetime"].getAsInstant())
            assertThat(atts["content"].asText()).isEqualTo("AAECAwQFBgcICQ==")
            assertThat(atts["json"].toString()).isEqualTo("{\"abc\":\"def\"}")
            assertThat(atts["binary"].asText()).isEqualTo("AAECAwQFBgcICQ==")
        }

        val srcRec = createRecord(srcAtts)
        checkAtts(srcRec, "text")

        val copyRec = updateRecord(
            srcRec,
            "id" to "copy-test",
            "text" to "copy-text"
        )
        assertThat(copyRec.id).isEqualTo("copy-test")
        checkAtts(srcRec, "text")
        checkAtts(copyRec, "copy-text")

        setAuthoritiesWithAttReadPerms(srcRec, "boolean", "boolean-reader")

        assertThrows<Exception> {
            AuthContext.runAs("user-0", emptyList()) {
                updateRecord(
                    srcRec,
                    "id" to "copy-test-2",
                    "text" to "copy-text-2"
                )
            }
        }

        val copyRec2 = AuthContext.runAs("user-1", listOf("boolean-reader")) {
            updateRecord(
                srcRec,
                "id" to "copy-test-2",
                "text" to "copy-text-2"
            )
        }
        checkAtts(copyRec2, "copy-text-2")
    }
}
