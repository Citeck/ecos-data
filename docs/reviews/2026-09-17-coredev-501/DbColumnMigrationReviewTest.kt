package ru.citeck.ecos.data.sql.test.records

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.batch.DbBatchTaskEngine
import ru.citeck.ecos.data.sql.props.DbEcosDataProps
import ru.citeck.ecos.data.sql.records.assocs.DbAssocEntity
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.webapp.api.entity.EntityRef
import java.time.Duration

class DbColumnMigrationReviewTest : DbRecordsTestBase() {

    init {
        dataProps = DbEcosDataProps(
            columns = DbEcosDataProps.ColumnsProps(inPlaceAlterMaxRows = 0),
            batch = DbEcosDataProps.BatchProps(batchPause = Duration.ZERO)
        )
    }

    private fun asType(type: AttributeType) {
        registerAtts(
            listOf(
                AttributeDef.create {
                    withId("att")
                    withType(type)
                    withMultiple(true)
                }
            )
        )
    }

    private fun links(ref: EntityRef): List<Long> = TxnContext.doInTxn {
        val schema = getTableCtx().getSchemaCtx()
        DbDataServiceImpl(
            DbAssocEntity::class.java,
            DbDataServiceConfig.create { withTable(DbAssocEntity.MAIN_TABLE) },
            schema
        ).findAll(Predicates.eq(DbAssocEntity.SOURCE_ID, dbRecordRefService.getIdByEntityRef(ref)))
            .map { it.targetId }
    }

    private fun drain() {
        val schema = getTableCtx().getSchemaCtx()
        DbBatchTaskEngine(schema.dataSourceCtx).drainSchemaOnce(schema)
    }

    @Test
    fun restartingSupersededDepartureMustNotRemoveLiveAssociations() {
        asType(AttributeType.ASSOC)
        val target = createRecord("att" to null)
        val source = createRecord("att" to listOf(target.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        val schema = getTableCtx().getSchemaCtx()
        val obsolete = TxnContext.doInTxn { schema.batchTaskService.findByTable(tableRef.table).single() }
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        assertThat(links(source)).containsExactly(dbRecordRefService.getIdByEntityRef(target))
        assertThat(schema.batchTaskService.restart(obsolete.id)).isTrue()
        DbBatchTaskEngine(schema.dataSourceCtx).runTask(schema, obsolete.id)
        assertThat(links(source)).containsExactly(dbRecordRefService.getIdByEntityRef(target))
    }

    @Test
    fun restoreMustRespectAssociationEditedBeforeBackgroundTransfer() {
        asType(AttributeType.ASSOC)
        val oldTarget = createRecord("att" to null)
        val newTarget = createRecord("att" to null)
        val source = createRecord("att" to listOf(oldTarget.toString()))
        asType(AttributeType.TEXT)
        createRecord("att" to null)
        drain()
        asType(AttributeType.ASSOC)
        createRecord("att" to null)
        records.mutate(source, mapOf("att" to listOf(newTarget.toString())))
        assertThat(links(source)).containsExactly(dbRecordRefService.getIdByEntityRef(newTarget))
        drain()
        assertThat(links(source)).containsExactly(dbRecordRefService.getIdByEntityRef(newTarget))
    }
}
