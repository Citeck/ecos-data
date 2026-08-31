package ru.citeck.ecos.data.sql.content.storage.local

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbTableRef
import ru.citeck.ecos.data.sql.meta.table.dto.DbTableMetaDto
import ru.citeck.ecos.data.sql.repo.entity.DbEntityMapper
import ru.citeck.ecos.data.sql.repo.find.DbFindPage
import ru.citeck.ecos.data.sql.repo.find.DbFindQuery
import ru.citeck.ecos.data.sql.repo.find.DbFindRes
import ru.citeck.ecos.data.sql.repo.find.DbFindSort
import ru.citeck.ecos.data.sql.service.DbDataService
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.model.lib.type.dto.QueryPermsPolicy
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.webapp.api.content.ContentRange
import ru.citeck.ecos.webapp.api.entity.EntityRef

class EcosContentLocalStorageRangeTest {

    companion object {
        private const val DATA_KEY = "ecd://local/1"
        private val CONTENT = "0123456789".toByteArray()
    }

    private val storageRef = EntityRef.EMPTY

    private fun createStorage(dataService: SingleEntityDataService): EcosContentLocalStorage {
        return EcosContentLocalStorage(dataService)
    }

    private fun readRange(range: ContentRange): String {
        val dataService = SingleEntityDataService(CONTENT)
        val result = createStorage(dataService).readContent(storageRef, DATA_KEY, range) { String(it.readBytes()) }
        // the blob column is read once, whatever the range is
        assertThat(dataService.findByIdCalls).isEqualTo(1)
        return result
    }

    @Test
    fun `a null range reads the whole blob`() {
        assertThat(readRange(ContentRange.UNBOUNDED)).isEqualTo("0123456789")
    }

    @Test
    fun `the plain read overload still reads the whole blob`() {
        val dataService = SingleEntityDataService(CONTENT)
        val result = createStorage(dataService).readContent(storageRef, DATA_KEY, ContentRange.UNBOUNDED) { String(it.readBytes()) }
        assertThat(result).isEqualTo("0123456789")
        assertThat(dataService.findByIdCalls).isEqualTo(1)
    }

    @Test
    fun `a range at the head of the blob`() {
        assertThat(readRange(ContentRange(0, 4))).isEqualTo("0123")
    }

    @Test
    fun `a range in the middle of the blob`() {
        assertThat(readRange(ContentRange(3, 4))).isEqualTo("3456")
    }

    @Test
    fun `a range from an offset until the end of the blob`() {
        assertThat(readRange(ContentRange.from(6))).isEqualTo("6789")
    }

    @Test
    fun `a range whose length overshoots the end of the blob is truncated`() {
        assertThat(readRange(ContentRange(7, 100))).isEqualTo("789")
    }

    @Test
    fun `a range covering more than the whole blob yields the whole blob`() {
        assertThat(readRange(ContentRange(0, 100))).isEqualTo("0123456789")
    }

    /**
     * `Long.MAX_VALUE` and `1L shl 32` both lose their meaning when narrowed to an int, so a length
     * which is not clamped to the available bytes before the narrowing turns "read everything from
     * here" into an empty stream.
     */
    @Test
    fun `a range whose length is larger than the whole int range yields the whole blob`() {
        assertThat(readRange(ContentRange(0, Long.MAX_VALUE))).isEqualTo("0123456789")
    }

    @Test
    fun `a range whose length overflows an int by a multiple of it yields the whole blob`() {
        assertThat(readRange(ContentRange(0, 1L shl 32))).isEqualTo("0123456789")
    }

    @Test
    fun `a range starting exactly at the end of the blob yields an empty stream`() {
        assertThat(readRange(ContentRange(CONTENT.size.toLong(), 5))).isEmpty()
    }

    @Test
    fun `a range starting past the end of the blob yields an empty stream`() {
        assertThat(readRange(ContentRange(CONTENT.size.toLong() + 100, 5))).isEmpty()
        assertThat(readRange(ContentRange.from(CONTENT.size.toLong() + 100))).isEmpty()
    }

    /**
     * Serves a single stored blob under id 1 and counts the reads. Everything the local storage
     * doesn't call throws.
     */
    private class SingleEntityDataService(
        data: ByteArray
    ) : DbDataService<DbContentDataEntity> {

        var findByIdCalls = 0
            private set

        private val entity = DbContentDataEntity().also {
            it.id = 1
            it.data = data
        }

        override fun findById(id: Long): DbContentDataEntity? {
            findByIdCalls++
            return if (id == entity.id) {
                entity
            } else {
                null
            }
        }

        override fun <T> doWithPermsPolicy(permsPolicy: QueryPermsPolicy?, action: () -> T): T = error("not used")
        override fun getSchemaVersion(): Int = error("not used")
        override fun setSchemaVersion(version: Int) = error("not used")
        override fun findByIds(ids: Set<Long>): List<DbContentDataEntity> = error("not used")
        override fun findByExtId(id: String): DbContentDataEntity? = error("not used")
        override fun findByExtId(id: String, expressions: Map<String, ExpressionToken>): DbContentDataEntity? =
            error("not used")
        override fun isExistsByExtId(id: String): Boolean = error("not used")
        override fun findAll(): List<DbContentDataEntity> = error("not used")
        override fun findAll(predicate: Predicate): List<DbContentDataEntity> = error("not used")
        override fun findAll(predicate: Predicate, sort: List<DbFindSort>): List<DbContentDataEntity> =
            error("not used")
        override fun find(
            predicate: Predicate,
            sort: List<DbFindSort>,
            page: DbFindPage
        ): DbFindRes<DbContentDataEntity> = error("not used")
        override fun find(
            predicate: Predicate,
            sort: List<DbFindSort>,
            page: DbFindPage,
            groupBy: List<String>,
            assocTableJoins: List<AssocTableJoin>,
            assocJoinWithPredicates: List<AssocJoinWithPredicate>,
            withTotalCount: Boolean
        ): DbFindRes<DbContentDataEntity> = error("not used")
        override fun find(query: DbFindQuery, page: DbFindPage): DbFindRes<DbContentDataEntity> = error("not used")
        override fun find(
            query: DbFindQuery,
            page: DbFindPage,
            withTotalCount: Boolean
        ): DbFindRes<DbContentDataEntity> = error("not used")
        override fun findRaw(
            predicate: Predicate,
            sort: List<DbFindSort>,
            page: DbFindPage,
            groupBy: List<String>,
            assocTableJoins: List<AssocTableJoin>,
            assocJoinWithPredicates: List<AssocJoinWithPredicate>,
            withTotalCount: Boolean
        ): DbFindRes<Map<String, Any?>> = error("not used")
        override fun findRaw(
            query: DbFindQuery,
            page: DbFindPage,
            withTotalCount: Boolean
        ): DbFindRes<Map<String, Any?>> = error("not used")
        override fun getCount(query: DbFindQuery): Long = error("not used")
        override fun getCount(predicate: Predicate): Long = error("not used")
        override fun save(entity: DbContentDataEntity): DbContentDataEntity = error("not used")
        override fun saveAtomicallyOrGetExistingByExtId(entity: DbContentDataEntity): Long = error("not used")
        override fun updateByExtIdIfMatches(
            extId: String,
            expected: Map<String, Any?>,
            newValues: Map<String, Any?>
        ): Boolean = error("not used")
        override fun save(entities: Collection<DbContentDataEntity>): List<DbContentDataEntity> = error("not used")
        override fun save(
            entities: Collection<DbContentDataEntity>,
            columns: List<DbColumnDef>
        ): List<DbContentDataEntity> = error("not used")
        override fun save(entity: DbContentDataEntity, columns: List<DbColumnDef>): DbContentDataEntity =
            error("not used")
        override fun delete(entity: DbContentDataEntity) = error("not used")
        override fun delete(predicate: Predicate) = error("not used")
        override fun delete(entityId: Long) = error("not used")
        override fun delete(entities: List<DbContentDataEntity>) = error("not used")
        override fun getTableRef(): DbTableRef = error("not used")
        override fun isTableExists(): Boolean = error("not used")
        override fun getTableMeta(): DbTableMetaDto = error("not used")
        override fun resetColumnsCache() = error("not used")
        override fun runMigrations(mock: Boolean, diff: Boolean): List<String> = error("not used")
        override fun runMigrations(
            expectedColumns: List<DbColumnDef>,
            mock: Boolean,
            diff: Boolean
        ): List<String> = error("not used")
        override fun getTableContext(): DbTableContext = error("not used")
        override fun getEntityMapper(): DbEntityMapper<DbContentDataEntity> = error("not used")
    }
}
