package ru.citeck.ecos.data.sql.columnmeta

import ru.citeck.ecos.data.sql.context.DbSchemaContext
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.DbDataServiceConfig
import ru.citeck.ecos.data.sql.service.DbDataServiceImpl
import ru.citeck.ecos.records2.predicate.model.Predicates
import ru.citeck.ecos.txn.lib.TxnContext

/**
 * The column registry of one schema: which semantic type each physical column of each domain table
 * of this schema actually holds.
 *
 * Internal machinery - there is no administrator-facing view of it. It is read
 * on every schema migration diff and written only when a column has actually been created,
 * converted or renamed into a backup.
 */
class DbColumnMetaService(schemaCtx: DbSchemaContext) {

    private val dataService: DbDataService<DbColumnMetaEntity> = DbDataServiceImpl(
        DbColumnMetaEntity::class.java,
        DbDataServiceConfig.create {
            withTable(DbColumnMetaEntity.TABLE)
        },
        schemaCtx
    )

    fun createTableIfNotExists() {
        TxnContext.doInTxn {
            dataService.runMigrations(mock = false, diff = true)
        }
    }

    /**
     * Every row the registry holds about one table, backups included.
     *
     * The transaction is not decoration. Until now every caller arrived from inside a mutation, so
     * the read joined a transaction that already existed; the background schema reconciliation calls
     * it from a scheduler thread where there is none at all, and on a managed XA datasource a
     * connection borrowed with no enclosing [TxnContext] transaction is never committed - see the
     * comment on [save] for the whole of that reasoning. `readOnly = true` joins an enclosing
     * writable transaction unchanged, so the mutation path pays nothing for it.
     */
    fun getByTable(table: String): List<DbColumnMetaDto> {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(Predicates.eq(DbColumnMetaEntity.TABLE_ID, table)).map { toDto(it) }
        }
    }

    /**
     * The registry as it describes the table's **live** columns, by column name. Backups are left
     * out: they describe a column that no longer answers for its attribute, so a caller comparing
     * the registry with the type model would see them as columns the model forgot to declare.
     */
    fun getLiveByTable(table: String): Map<String, DbColumnMetaDto> {
        return getByTable(table).asSequence()
            .filter { !it.backup }
            .associateBy { it.columnName }
    }

    /**
     * Every backup of one attribute, **newest first**. A restore takes the freshest backup whose type
     * matches the type being returned to, so the order is part of the contract.
     *
     * **Ordered by `__created`, with the row id only as a tiebreak**, because a backup row's id is
     * not minted when it becomes a backup: the row describing a backup column is the row that
     * described it while it was live, so its id records when the *column* was created. Two ordinary
     * things move a row's role without minting anything - a restore makes an old row live again, and
     * a relabel (`TEXT <-> OPTIONS`, or any move between assoc-like types) changes a live row's type
     * in place - and between them a low-id row can be the freshest backup of its type.
     *
     * `__created` keeps meaning "when this row last became what it now is", and one clause holds
     * that up: `moveAside` and `restore` stamp every row whose role they change, and **no other
     * writer can reach a row that is already a backup**. `DbDataServiceImpl.writeColumnMeta` looks a
     * row up by attribute id, while every backup column's name carries the `__backup_` prefix and an
     * attribute id may not begin with `_`. Key `writeColumnMeta` on anything but the attribute id
     * and this ordering goes with it.
     */
    fun findBackups(table: String, attId: String): List<DbColumnMetaDto> {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbColumnMetaEntity.TABLE_ID, table),
                    Predicates.eq(DbColumnMetaEntity.ATT_ID, attId),
                    Predicates.eq(DbColumnMetaEntity.BACKUP, true)
                ),
                listOf(
                    DbFindSort(DbColumnMetaEntity.CREATED, false),
                    // a total order, so that the answer is the same on every call: two backups of
                    // one attribute would have to change role inside the same clock tick to tie,
                    // and each such change is a round trip to the database of its own
                    DbFindSort(DbColumnMetaEntity.ID, false)
                )
            ).map { toDto(it) }
        }
    }

    /**
     * The registry row of one physical column, or null when the registry has never seen it - which
     * on an upgraded installation simply means seeding has not reached this table yet.
     */
    fun getByTableAndColumn(table: String, columnName: String): DbColumnMetaDto? {
        return TxnContext.doInTxn(readOnly = true) {
            dataService.findAll(
                Predicates.and(
                    Predicates.eq(DbColumnMetaEntity.TABLE_ID, table),
                    Predicates.eq(DbColumnMetaEntity.COLUMN_NAME, columnName)
                ),
                listOf(DbFindSort(DbColumnMetaEntity.ID, false))
            ).firstOrNull()?.let { toDto(it) }
        }
    }

    fun save(dto: DbColumnMetaDto): DbColumnMetaDto {
        // Explicit REQUIRED-policy transaction, not just dataService's own internal one: on any
        // managed XA datasource - not just the test backends, production included - a connection
        // borrowed with no enclosing ru.citeck.ecos.txn.lib.TxnContext transaction is never
        // committed (commit is left to the XA manager, which only runs when a TxnContext
        // transaction is actually active). Joins an outer transaction when the caller already has
        // one, exactly like createTableIfNotExists.
        return TxnContext.doInTxn {
            toDto(dataService.save(toEntity(dto)))
        }
    }

    fun saveAll(dtos: List<DbColumnMetaDto>): List<DbColumnMetaDto> {
        if (dtos.isEmpty()) {
            return emptyList()
        }
        return TxnContext.doInTxn {
            dataService.save(dtos.map { toEntity(it) }).map { toDto(it) }
        }
    }

    fun resetColumnsCache() {
        dataService.resetColumnsCache()
    }

    private fun toDto(entity: DbColumnMetaEntity): DbColumnMetaDto {
        return DbColumnMetaDto(
            id = entity.id,
            table = entity.table,
            columnName = entity.columnName,
            attId = entity.attId,
            attType = DbColumnSemanticType.parse(entity.attType),
            multiple = entity.multiple,
            backup = entity.backup,
            created = entity.created,
            creator = entity.creator
        )
    }

    private fun toEntity(dto: DbColumnMetaDto): DbColumnMetaEntity {
        val entity = DbColumnMetaEntity()
        entity.id = dto.id
        entity.table = dto.table
        entity.columnName = dto.columnName
        entity.attId = dto.attId
        entity.attType = dto.attType.asString()
        entity.multiple = dto.multiple
        entity.backup = dto.backup
        entity.created = dto.created
        entity.creator = dto.creator
        return entity
    }
}
