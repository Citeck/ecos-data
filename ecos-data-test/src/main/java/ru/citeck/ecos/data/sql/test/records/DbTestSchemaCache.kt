package ru.citeck.ecos.data.sql.test.records

import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext

/**
 * The switch behind the record-suite's schema cache.
 *
 * Every test of the suite used to drop the whole database in `@AfterEach` and let the next test
 * build it again from nothing. On PostgreSQL that rebuild - `runSchemaMigrations` plus the fourteen
 * `ed_*` system tables and their indexes - is the single largest cost of the module, and it is
 * pure repetition: the state it produces is identical for every test in the run. The cache keeps
 * that state and cleans the *data* instead (see the backend's
 * [DbRecordsTestBackend.resetForNextTest]).
 *
 * There are two independent ways to get the old, uncached behaviour back, and they exist for
 * different reasons:
 *
 *  - **the global flag** `-Decos.data.test.reuseSchema=false` (this object's [isReuseEnabled]).
 *    It is a system property, not a Maven property, so the same switch works from `mvn`, from an
 *    IDE run configuration and from a single `@Test` run. It is the "guarantee me a clean room"
 *    button: with it off the suite behaves exactly as it did before the cache existed, which is
 *    what a release run should do.
 *
 *  - **the per-class opt-out** [RequiresFreshSchema]. That is *not* a special case of the flag:
 *    a class carrying it gets a genuinely fresh database whatever the flag says, because what it
 *    tests **is** the schema-creation path itself. Turning the flag off makes every class fresh;
 *    the annotation makes one class fresh even in a fast, fully cached run - which is the run
 *    developers actually do all day, and therefore the run in which those classes must not
 *    silently start observing a schema somebody else created.
 */
object DbTestSchemaCache {

    /**
     * `-Decos.data.test.reuseSchema=false` restores the drop-and-recreate behaviour for the whole
     * JVM. Default on: the fast path is the one we want to be the norm, and the acceptance
     * criterion is that the two modes agree.
     */
    const val REUSE_SCHEMA_PROP = "ecos.data.test.reuseSchema"

    private var forcedFreshDepth = 0

    /**
     * Wipes whatever the cache is currently holding, physically. Registered by the backend that
     * owns a cacheable database (today only the PostgreSQL one); null when the JVM has not created
     * such a backend yet, in which case there is by definition nothing cached to wipe.
     */
    @Volatile
    private var cacheInvalidator: (() -> Unit)? = null

    fun isReuseEnabled(): Boolean {
        return System.getProperty(REUSE_SCHEMA_PROP, "true").toBoolean()
    }

    /**
     * Whether the caller may reuse an already-initialised schema right now: the flag is on **and**
     * no [RequiresFreshSchema] class is currently running.
     */
    @Synchronized
    fun isSchemaReuseActive(): Boolean {
        return forcedFreshDepth == 0 && isReuseEnabled()
    }

    /**
     * Called by the backend as soon as it can wipe its database, so that [RequiresFreshSchema] can
     * be honoured for a test class which never touches [DataMockFactory] at all (the schema
     * migration suites build their own data source) and therefore has no other way to reach the
     * cached state.
     */
    @Synchronized
    fun registerCacheInvalidator(invalidator: () -> Unit) {
        cacheInvalidator = invalidator
    }

    @Synchronized
    fun beginForcedFreshSchema() {
        forcedFreshDepth++
        cacheInvalidator?.invoke()
    }

    @Synchronized
    fun endForcedFreshSchema() {
        if (forcedFreshDepth > 0) {
            forcedFreshDepth--
        }
        cacheInvalidator?.invoke()
    }
}

/**
 * Marks a test class which must see a database nobody else has initialised, **regardless of
 * [DbTestSchemaCache.REUSE_SCHEMA_PROP]**. Use it when the schema-creation or schema-migration
 * path is the thing under test: a cached schema is already at the newest version, so
 * `runSchemaMigrations` returns without doing anything and an assertion about what a migration
 * does would pass vacuously - the worst kind of green.
 *
 * It is deliberately a separate mechanism from the global flag: the flag is a run-wide policy a
 * developer flips, the annotation is a property of the test that no policy may override.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(FreshSchemaExtension::class)
annotation class RequiresFreshSchema

/**
 * Implements [RequiresFreshSchema]: suspends the cache for the duration of the class and wipes the
 * cached database both before the class starts (so it does not inherit somebody else's schema) and
 * after it finishes (so it does not hand its own leftovers to the next class, which will happily
 * adopt them as the new cache baseline otherwise).
 */
class FreshSchemaExtension :
    BeforeAllCallback,
    AfterAllCallback {

    override fun beforeAll(context: ExtensionContext) {
        DbTestSchemaCache.beginForcedFreshSchema()
    }

    override fun afterAll(context: ExtensionContext) {
        DbTestSchemaCache.endForcedFreshSchema()
    }
}
