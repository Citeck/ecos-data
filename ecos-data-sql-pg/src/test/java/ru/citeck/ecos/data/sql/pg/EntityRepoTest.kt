package ru.citeck.ecos.data.sql.pg

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.data.sql.datasource.DbDataSource
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.data.sql.pg.repo.SqlDataServiceTestUtils
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.records2.predicate.model.ValuePredicate
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong

class EntityRepoTest {

    companion object {
        private const val STR_COLUMN = "str_column"
        private const val STR_COLUMN_V0 = "str_column_value_0"
        private const val STR_COLUMN_V1 = "str_column_value_1"
        private const val STR_COLUMN_V2 = "str_column_value_2"

        private val refIdCounter = AtomicLong()
    }

    @Test
    fun test() {
        PgUtils.withDbDataSource {
            testArrays(it)
            testImpl(it)
        }
    }

    private fun newEntity(): DbEntity {
        val entity = DbEntity()
        entity.refId = refIdCounter.getAndIncrement()
        return entity
    }

    private fun testImpl(dbDataSource: DbDataSource) {

        val columns = listOf(
            DbColumnDef.create {
                withName(STR_COLUMN)
                withType(DbColumnType.TEXT)
            }
        )

        val context = SqlDataServiceTestUtils.createService(dbDataSource, "test")

        var findRes = context.service.findAll()

        assertThat(findRes).isEmpty()

        var newEntity = newEntity()
        newEntity.attributes[STR_COLUMN] = STR_COLUMN_V0
        val instantBeforeSave = Instant.now().truncatedTo(ChronoUnit.MILLIS)

        newEntity = context.service.save(newEntity, columns)

        assertThat(newEntity.extId).isNotBlank()
        // this fields now filled in RecordsDao
        // assertThat(newEntity.creator).isEqualTo(AuthContext.getCurrentUser())
        // assertThat(newEntity.modifier).isEqualTo(AuthContext.getCurrentUser())

        // assertThat(newEntity.modified)
        //     .isEqualTo(newEntity.created)
        //     .isAfterOrEqualTo(instantBeforeSave)

        assertThat(newEntity.deleted).isFalse
        assertThat(newEntity.status).isEmpty()

        findRes = context.service.findAll()

        assertThat(findRes).hasSize(1)
        assertThat(findRes[0].attributes[STR_COLUMN]).isEqualTo(STR_COLUMN_V0)

        val entityById = context.service.findByExtId(newEntity.extId) ?: error("Entity is null: ${newEntity.extId}")
        assertThat(entityById.attributes[STR_COLUMN]).isEqualTo(STR_COLUMN_V0)

        val findByPredicateEqRes = context.service.findAll(ValuePredicate.eq(STR_COLUMN, STR_COLUMN_V0))
        assertThat(findByPredicateEqRes).hasSize(1)
        assertThat(findByPredicateEqRes[0].attributes[STR_COLUMN]).isEqualTo(STR_COLUMN_V0)

        val findByPredicateContainsRes = context.service.findAll(ValuePredicate.contains(STR_COLUMN, "value_0"))
        assertThat(findByPredicateContainsRes).hasSize(1)

        val findByPredicateContainsUnknownRes = context.service.findAll(ValuePredicate.contains(STR_COLUMN, "unknown"))
        assertThat(findByPredicateContainsUnknownRes).isEmpty()
    }

    fun testArrays(dbDataSource: DbDataSource) {

        val columns = listOf(
            DbColumnDef.create {
                withName(STR_COLUMN)
                withType(DbColumnType.TEXT)
                withMultiple(true)
            }
        )

        val context = SqlDataServiceTestUtils.createService(dbDataSource, "test-arrays")

        var newEntity0 = newEntity()
        newEntity0.attributes[STR_COLUMN] = STR_COLUMN_V0

        newEntity0 = context.service.save(newEntity0, columns)

        assertThat(newEntity0.extId).isNotBlank
        assertThat(newEntity0.attributes[STR_COLUMN] as List<*>).containsExactlyInAnyOrderElementsOf(
            listOf(STR_COLUMN_V0)
        )

        newEntity0.attributes[STR_COLUMN] = listOf(STR_COLUMN_V0, STR_COLUMN_V1, STR_COLUMN_V2)
        newEntity0 = context.service.save(newEntity0, columns)

        assertThat(newEntity0.attributes[STR_COLUMN] as List<*>).containsExactlyInAnyOrderElementsOf(
            listOf(STR_COLUMN_V0, STR_COLUMN_V1, STR_COLUMN_V2)
        )

        assertThrows<Exception> {
            context.service.save(newEntity0, columns)
            // concurrent modification exception
            context.service.save(newEntity0, columns)
        }

        var newEntity1 = newEntity()
        newEntity1.attributes[STR_COLUMN] = listOf(STR_COLUMN_V0, STR_COLUMN_V1, STR_COLUMN_V2)

        newEntity1 = context.service.save(newEntity1, columns)
        assertThat(newEntity1.attributes[STR_COLUMN] as List<*>).containsExactlyInAnyOrderElementsOf(
            listOf(STR_COLUMN_V0, STR_COLUMN_V1, STR_COLUMN_V2)
        )
    }
}
