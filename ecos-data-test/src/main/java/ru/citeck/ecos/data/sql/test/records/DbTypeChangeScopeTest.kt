package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import ru.citeck.ecos.data.sql.ecostype.DbEcosModelService
import ru.citeck.ecos.data.sql.modelchange.DbTypeChangeScope
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.concurrent.TimeUnit

/**
 * A model change names a type; what has to be reconciled is a table. Everything asserted here is
 * about that gap: a type without storage of its own must still lead to the table that holds its
 * records, and a type with its own storage must not drag its ancestor's table along.
 */
class DbTypeChangeScopeTest : DbRecordsTestBase() {

    private fun scope(): DbTypeChangeScope {
        return DbTypeChangeScope(
            DbEcosModelService(modelServiceFactory),
            dataSourceCtx.recordsDaoIndex
        )
    }

    private fun registerMainType() {
        registerAtts(listOf(AttributeDef.create { withId("textAtt") }))
    }

    @Test
    fun typeWithItsOwnDaoResolvesToThatDaoTest() {

        registerMainType()

        assertThat(scope().daosForTypeChange(REC_TEST_TYPE_ID)).containsExactly(mainCtx.dao)
    }

    /**
     * The case the step up exists for. The child has no `sourceId` of its own, so it inherits the
     * parent's, gets no DAO, and its records land in the parent's table. Without the walk the
     * answer here is an empty list and the table is never reconciled by the trigger.
     */
    @Test
    fun descendantWithoutItsOwnStorageResolvesToTheAncestorsDaoTest() {

        registerMainType()
        registerType(
            TypeInfo.create {
                withId("child-without-storage")
                withParentRef(REC_TEST_TYPE_REF)
            }
        )

        assertThat(dataSourceCtx.recordsDaoIndex.getByTypeId("child-without-storage")).isEmpty()
        assertThat(scope().daosForTypeChange("child-without-storage")).containsExactly(mainCtx.dao)
    }

    /**
     * The other direction of the same boundary: a different `sourceId` means a different table, so
     * the walk must stop at the child and the parent's DAO must not be dragged in.
     */
    @Test
    fun descendantWithItsOwnStorageResolvesOnlyToItsOwnDaoTest() {

        registerMainType()
        val ownCtx = registerType()
            .asSubTypeWithId("child-with-storage")
            .withSourceId("child-with-storage-dao")
            .register()

        assertThat(scope().daosForTypeChange("child-with-storage")).containsExactly(ownCtx.dao)
    }

    /**
     * A parent chain that loops back on itself. Nothing in the type model forbids it, the upward
     * walk has no visited-set, and the only thing standing between it and an infinite loop is the
     * step cap - so this asserts both that the call returns at all and that the repeated ids it
     * collects on the way out collapse to one DAO.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun cycleInTheTypeTreeTerminatesTest() {

        registerType(
            TypeInfo.create {
                withId("cycle-a")
                withParentRef(ModelUtils.getTypeRef("cycle-b"))
                withSourceId("cycle-dao")
            }
        )
        registerType(
            TypeInfo.create {
                withId("cycle-b")
                withParentRef(ModelUtils.getTypeRef("cycle-a"))
                withSourceId("cycle-dao")
            }
        )
        val cycleCtx = createRecordsDao(
            DEFAULT_TABLE_REF.withTable("cycle-table"),
            ModelUtils.getTypeRef("cycle-b"),
            "cycle-dao"
        )

        assertThat(scope().daosForTypeChange("cycle-a")).containsExactly(cycleCtx.dao)
    }

    /**
     * Also covers the broken-chain exit of the walk: `no-dao-parent` names an ancestor which does
     * not exist, so the climb stops there rather than on a `sourceId` change.
     */
    @Test
    fun typeWithNoDaoAtAnyLevelGivesAnEmptyListTest() {

        registerType(
            TypeInfo.create {
                withId("no-dao-parent")
                withParentRef(ModelUtils.getTypeRef("missing-ancestor"))
                withSourceId("no-dao-source")
            }
        )
        registerType(
            TypeInfo.create {
                withId("no-dao-child")
                withParentRef(ModelUtils.getTypeRef("no-dao-parent"))
                withSourceId("no-dao-source")
            }
        )

        assertThat(scope().daosForTypeChange("no-dao-child")).isEmpty()
    }

    /**
     * A type can be deleted between the moment its change is announced and the moment it is
     * processed, so an unknown id is a normal outcome and not an error.
     */
    @Test
    fun unknownTypeGivesAnEmptyListTest() {

        assertThat(scope().daosForTypeChange("type-deleted-before-the-tick")).isEmpty()
    }

    /**
     * The climb is shared by two callers with different consequences, and
     * [ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog.warnOnce] deduplicates per key for
     * the whole life of the JVM - so a key shared by both paths means the first caller to hit the
     * cap permanently silences the other, for a type whose table is then never reconciled and
     * whose operator is never told why.
     *
     * Asserted in the order that fails when the keys are shared: the conflict path warns first,
     * and the trigger path - the one whose consequence is the worse of the two - has to be heard
     * afterwards, about the very same type.
     */
    @Test
    fun aCappedClimbIsReportedOncePerPathAndNotOncePerTypeTest() {

        // one longer than the 30-step cap, all sharing one storage, so the climb from the deepest
        // one runs out of steps before it reaches the top
        val chainLength = 31
        for (i in 0 until chainLength) {
            registerType(
                TypeInfo.create {
                    withId("capped-chain-$i")
                    withParentRef(ModelUtils.getTypeRef("capped-chain-${i + 1}"))
                }
            )
        }
        val modelService = DbEcosModelService(modelServiceFactory)
        val deepest = modelService.getTypeInfo("capped-chain-0")!!

        val conflictsPathLog = captureStderr { modelService.getTableAttTypes(deepest) }
        val triggerPathLog = captureStderr { modelService.getStorageBoundaryTypeIds(deepest) }

        assertThat(conflictsPathLog)
            .describedAs("the mutation path says what an incomplete climb costs it")
            .contains("Conflicts for its table may be under-detected")
        assertThat(triggerPathLog)
            .describedAs("and the trigger path must not be silenced by it - its consequence is a different one")
            .contains("capped-chain-0")
            .contains("may not reach the table")
    }

    /**
     * slf4j-simple (the test binding) writes to stderr and this project has no in-JVM log capture
     * utility - the same approach [DbShadowColumnTransitionTest] uses.
     */
    private fun captureStderr(action: () -> Unit): String {
        val original = System.err
        val captured = ByteArrayOutputStream()
        System.setErr(PrintStream(captured, true))
        try {
            action()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }
}
