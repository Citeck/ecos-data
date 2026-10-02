package ru.citeck.ecos.data.sql.migration.column

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

class DbColumnMigrationParamsTest {
    private fun params() = DbColumnMigrationParams(
        "value", "backup_json", "value", DbColumnSemanticType.Model(AttributeType.JSON), false,
        AttributeType.MLTEXT, false, false, true, DbConversionClass.LOSSY, 2
    )

    @Test
    fun compactSourcesRoundTripWithoutTaskFields() {
        val source = DbColumnMigrationSource("backup_text", 1, DbColumnSemanticType.Model(AttributeType.TEXT), false)
        val params = params().copy(fallbackSources = listOf(source))
        val stored = params.toObjectData()
        val storedSource = stored.get("fallbackSources")[0].asObjectData()
        assertThat(storedSource.has("targetType")).isFalse()
        assertThat(storedSource.has("targetIndexEnabled")).isFalse()
        assertThat(storedSource.has("fallbackSources")).isFalse()
        assertThat(DbColumnMigrationParams.from(stored)).isEqualTo(params)
        assertThat(params.forSource(source).conversionClass).isEqualTo(DbConversionClass.SAFE)
    }

    @Test
    fun legacyFullTaskSourcesAreReadAsSourcesOnly() {
        val source = params().copy(
            backupColumn = "backup_assoc",
            backupColumnMetaId = 1,
            sourceType = DbColumnSemanticType.Model(AttributeType.ASSOC),
            sourceMultiple = true,
            targetType = AttributeType.TEXT
        ).toObjectData()
        source.set("sourceAssocBackup", true)
        val stored = params().toObjectData()
        stored.set("fallbackSources", listOf(source))
        val restored = DbColumnMigrationParams.from(stored)
        assertThat(restored.fallbackSources).containsExactly(
            DbColumnMigrationSource("backup_assoc", 1, DbColumnSemanticType.Model(AttributeType.ASSOC), true, true)
        )
        assertThat(restored.forSource(restored.fallbackSources.single()).targetType).isEqualTo(AttributeType.MLTEXT)
    }

    @Test
    fun tasksWithoutFallbackSourcesKeepTheirOriginalSource() {
        val stored = params().toObjectData()
        stored.remove("fallbackSources")
        assertThat(DbColumnMigrationParams.from(stored)).isEqualTo(params())
    }
    @Test
    fun primarySourceKeepsTheConversionDecisionStoredWithTheTask() {
        val recorded = params().copy(conversionClass = DbConversionClass.NONE)
        assertThat(recorded.forSource(recorded.sources().first()).conversionClass).isEqualTo(DbConversionClass.NONE)
        assertThat(recorded.hasTransferableSource()).isFalse()
    }
}
