package ru.citeck.ecos.data.sql.pg.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.context.lib.auth.AuthGroup
import ru.citeck.ecos.context.lib.auth.data.EmptyAuth
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.model.lib.role.constants.RoleConstants
import ru.citeck.ecos.records2.RecordRef
import ru.citeck.ecos.txn.lib.TxnContext
import kotlin.collections.ArrayList

class DbRecordsAuthTest : DbRecordsTestBase() {

    @BeforeEach
    fun beforeEach() {

        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("textAtt")
                    withType(AttributeType.TEXT)
                },
                AttributeDef.create {
                    withId("num")
                    withType(AttributeType.NUMBER)
                }
            )
        )
        initServices(tableRef, true)
    }

    @Test
    fun txnTest() {

        var rec: RecordRef = RecordRef.EMPTY
        val query = createQuery()

        val queryRecAssert = {
            assertThat(records.query(query).getRecords()[0]).isEqualTo(rec)
        }
        val emptyQueryResAssert = {
            val queryRes = records.query(query)
            assertThat(queryRes.getTotalCount()).isEqualTo(0L)
            assertThat(queryRes.getRecords()).isEmpty()
        }

        AuthContext.runAs("user0") {
            TxnContext.doInTxn {
                rec = createRecord("textAtt" to "value")
                assertThat(records.getAtt(rec, "textAtt").asText()).isEqualTo("value")
                // New record in txn table, and we can't find it. Maybe in future this will be changed.
                emptyQueryResAssert()
            }
            setAuthoritiesWithReadPerms(rec, "user0")
            assertThat(records.getAtt(rec, "textAtt").asText()).isEqualTo("value")
            queryRecAssert()
        }

        AuthContext.runAs("user1") {
            assertThat(records.getAtt(rec, "textAtt").asText()).isEqualTo("")
            emptyQueryResAssert()
        }
    }

    @Test
    fun queryTest() {

        val recsByUser = mutableMapOf<String, MutableList<RecordRef>>()

        val users = (1..10).map { "user$it" }
        for (user in users) {
            AuthContext.runAs(user) {
                val recs = recsByUser.computeIfAbsent(user) { ArrayList() }
                repeat(5) { num ->
                    val recId = "user-rec-$user-$num"
                    val rec = createRecord(
                        "_localId" to recId,
                        "textAtt" to "123-$user-$num"
                    )
                    assertThat(rec.id).isEqualTo(recId)
                    setAuthoritiesWithReadPerms(rec, user)
                    recs.add(rec)
                }
            }
        }
        val baseQuery = createQuery()

        for (user in users) {
            AuthContext.runAs(user) {
                val records = records.query(baseQuery).getRecords()
                assertThat(records).containsExactlyInAnyOrderElementsOf(recsByUser[user])
            }
        }

        val refForEveryone = createRef("user-rec-user5-2")

        listOf(RoleConstants.ROLE_EVERYONE, AuthGroup.EVERYONE).forEach { allUsersAuthority ->
            setAuthoritiesWithReadPerms(refForEveryone, allUsersAuthority)
            for (user in users) {
                AuthContext.runAs(user) {
                    val records = records.query(baseQuery).getRecords()
                    val recsWithPublicRef = recsByUser[user]!!.toMutableSet()
                    recsWithPublicRef.add(refForEveryone)
                    assertThat(records)
                        .describedAs(allUsersAuthority)
                        .containsExactlyInAnyOrderElementsOf(recsWithPublicRef)
                }
            }
        }

        AuthContext.runAs("user5") {
            records.delete(refForEveryone)
        }

        for (user in users) {
            AuthContext.runAs(user) {
                val records = records.query(baseQuery).getRecords()
                val recsWithPublicRef = recsByUser[user]!!.toMutableSet()
                if (user == "user5") {
                    recsWithPublicRef.remove(refForEveryone)
                }
                assertThat(records).containsExactlyInAnyOrderElementsOf(recsWithPublicRef)
            }
        }

        AuthContext.runAs("user6") {
            val user6Recs = recsByUser["user6"]!!
            for (i in 0..4) {
                val recsQueryRes = records.query(
                    baseQuery.copy {
                        withMaxItems(1)
                        withSkipCount(i)
                    }
                )
                assertThat(recsQueryRes.getRecords()).describedAs("it-$i").hasSize(1)
                assertThat(recsQueryRes.getTotalCount()).describedAs("it-$i").isEqualTo(user6Recs.size.toLong())
                assertThat(recsQueryRes.getHasMore()).describedAs("it-$i").isEqualTo(i < 4)
                assertThat(recsQueryRes.getRecords()[0]).describedAs("it-$i").isEqualTo(user6Recs[i])
            }
        }

        printQueryRes("SELECT * FROM \"records-test-schema\".\"ecos_authorities\"")
    }

    @Test
    fun testWithoutContext() {

        var rec: RecordRef = RecordRef.EMPTY
        AuthContext.runAs("user0") {
            rec = createRecord("textAtt" to "123")
            assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("123")
        }

        setAuthoritiesWithReadPerms(rec, "user0")

        AuthContext.runAs(EmptyAuth) {

            assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("")
            AuthContext.runAs("user1") {
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("")
                assertThrows<Exception> {
                    updateRecord(rec, "textAtt" to "567", "_type" to REC_TEST_TYPE_REF)
                }
                AuthContext.runAsFull("user0") {
                    updateRecord(rec, "textAtt" to "111")
                    assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("111")
                }
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("")
                assertThrows<Exception> {
                    updateRecord(rec, "textAtt" to "567", "_type" to REC_TEST_TYPE_REF)
                }
            }

            setAuthoritiesWithReadPerms(rec, setOf("user0", "user1"))
            AuthContext.runAs("user0") {
                updateRecord(rec, "textAtt" to "with perms")
            }
            AuthContext.runAs("user1") {
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("with perms")
            }
            AuthContext.runAs("user3") {
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("")
            }

            setAuthoritiesWithReadPerms(rec, (0..10).map { "user$it" }.toSet())
            AuthContext.runAs("user1") {
                updateRecord(rec, "textAtt" to "update-perms")
            }
            AuthContext.runAs("user5") {
                updateRecord(rec, "textAtt" to "val22")
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("val22")
            }

            setAuthoritiesWithReadPerms(rec, (0..5).map { "user$it" }.toSet())
            AuthContext.runAs("user5") {
                updateRecord(rec, "textAtt" to "val33")
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("val33")
            }

            setAuthoritiesWithReadPerms(rec, (0..4).map { "user$it" }.toSet())
            AuthContext.runAs("user5") {
                assertThat(records.getAtt(rec, "textAtt?str").asText()).isEqualTo("")
            }
        }
    }

    @Test
    fun testWithRevokeOwnPerms() {

        val testId = "test-id"
        val setRecPerms = { perms: List<String> ->
            setAuthoritiesWithReadPerms(RecordRef.create(recordsDao.getId(), testId), perms)
        }
        setRecPerms(listOf("user0", "user1"))

        val rec = AuthContext.runAs("user0") {
            createRecord(
                "id" to testId,
                "textAtt" to "123"
            )
        }

        val testTextAttValueAsUser0 = { value: String ->
            val attValue = AuthContext.runAs("user0") {
                records.getAtt(rec, "textAtt").asText()
            }
            assertThat(attValue).isEqualTo(value)
        }

        testTextAttValueAsUser0("123")

        AuthContext.runAs("user1") {
            TxnContext.doInTxn {
                records.mutateAtt(rec, "textAtt", "1234")
            }
        }

        testTextAttValueAsUser0("1234")

        AuthContext.runAs("user1") {
            TxnContext.doInTxn {
                records.mutateAtt(rec, "textAtt", "1234")
                setRecPerms(listOf("user0"))
                records.mutateAtt(rec, "textAtt", "123456")
            }
        }

        testTextAttValueAsUser0("123456")
    }
}
