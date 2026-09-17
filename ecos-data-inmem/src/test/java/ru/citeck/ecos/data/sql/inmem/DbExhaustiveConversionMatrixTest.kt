package ru.citeck.ecos.data.sql.inmem

import org.junit.jupiter.params.provider.Arguments
import ru.citeck.ecos.data.sql.test.records.DbConversionMatrixTestBase
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.util.stream.Stream

/**
 * The whole of spec section 5, on both of its axes and with nothing sampled: **every**
 * [AttributeType] to every [AttributeType], and source multiplicity to target multiplicity
 * independently - 15 x 15 x 2 x 2 changes, less the 30 that change nothing at all, which
 * `anUnchangedModelMovesNothingTest` covers in two fixtures instead of thirty.
 *
 * Independent multiplicities are the point of the second pair of dimensions: `(type, true) ->
 * (type, false)` is the narrowing that keeps the first element and leaves the rest
 * in the backup, and a matrix that used one flag for both sides would never reach it.
 *
 * It lives in the in-memory module's own tests rather than in the shared cross-backend suite, and
 * that is a decision about cost, not about coverage. Every case creates records and drains the batch
 * engine, which is milliseconds in memory and about 0.6 seconds a case on PostgreSQL through
 * Testcontainers - so this sweep there would cost more than the entire three-module suite does
 * today. The in-memory backend is the right place for it precisely because it expresses **no**
 * conversion of its own: everything it does is the universal row-by-row path required of
 * every backend, so a change that behaves differently here is one that leaked into a
 * backend-specific path.
 *
 * `DbConversionMatrixTest` runs the same three statements on **both** backends over a set the map's
 * own groups make representative. That set is sampled, not exhaustive, and its own KDoc says which
 * axis is sampled how.
 */
class DbExhaustiveConversionMatrixTest : DbConversionMatrixTestBase() {

    companion object {

        @JvmStatic
        fun pairs(): Stream<Arguments> {
            val result = ArrayList<Arguments>()
            for (from in AttributeType.entries) {
                for (to in AttributeType.entries) {
                    for (sourceMultiple in listOf(false, true)) {
                        for (targetMultiple in listOf(false, true)) {
                            if (from == to && sourceMultiple == targetMultiple) {
                                continue
                            }
                            result.add(Arguments.of(from, sourceMultiple, to, targetMultiple))
                        }
                    }
                }
            }
            return result.stream()
        }
    }
}
