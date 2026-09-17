package ru.citeck.ecos.data.sql.test.records

import org.junit.jupiter.params.provider.Arguments
import ru.citeck.ecos.data.sql.columnmeta.DbConversionClass
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType
import java.util.stream.Stream

/**
 * The conversion map on **both** backends, over a set the map's own groups make representative.
 *
 * The exhaustive sweep lives in `DbExhaustiveConversionMatrixTest`, where the in-memory backend makes
 * it cost seconds; on PostgreSQL it would take longer than the whole suite. What has to run on both
 * is the statement the sweep cannot make - that the answers are the **map's** and not PostgreSQL's -
 * and a representative set suffices for that.
 *
 * **What is exhausted and what is sampled**, since a matrix that does not say so is read as covering
 * everything:
 *
 *  - **the type axis is exhausted for SAFE** - that is the class promising the user something, and
 *    the only one the in-place path may take - **and sampled by group for the rest**
 *    ([conversionGroupOf]). LOSSY is sampled **per pair of groups**, source and target, because a
 *    rule can be about a particular pair and the MLTEXT ones are: sampling by source alone would
 *    hand MLTEXT's single slot to whichever target came first. NONE is sampled **per source group**,
 *    because "nothing is transferred, the backup is permanent" is the same statement whatever the
 *    target - now asserted rather than assumed, which is what makes that sampling sound;
 *  - **the multiplicity axis is sampled one attribute type per physical column type**
 *    ([oneAttributeTypePerColumnType]), widened and narrowed: what an array of a given physical
 *    column does is not changed by the attribute type sitting on it. The cross product of type and
 *    arity change is the exhaustive class's job.
 *
 * The selection is computed from [classOf] at run time, so a rule that changes a class moves the
 * change into or out of this suite by itself.
 */
class DbConversionMatrixTest : DbConversionMatrixTestBase() {

    companion object {

        @JvmStatic
        fun pairs(): Stream<Arguments> {
            val result = LinkedHashSet<List<Any>>()
            for (multiple in listOf(false, true)) {
                val representedLossy = HashSet<Pair<String, String>>()
                val representedNone = HashSet<String>()
                for (from in AttributeType.entries) {
                    for (to in AttributeType.entries) {
                        if (from == to) {
                            // an unchanged model is anUnchangedModelMovesNothingTest's subject, and
                            // a change of arity alone is carried by the representatives below
                            continue
                        }
                        val included = when (classOf(from, multiple, to, multiple)) {
                            DbConversionClass.SAFE -> true
                            DbConversionClass.LOSSY ->
                                representedLossy.add(conversionGroupOf(from) to conversionGroupOf(to))
                            DbConversionClass.NONE -> representedNone.add(conversionGroupOf(from))
                        }
                        if (included) {
                            result.add(listOf(from, multiple, to, multiple))
                        }
                    }
                }
            }
            for (type in oneAttributeTypePerColumnType()) {
                result.add(listOf(type, false, type, true))
                result.add(listOf(type, true, type, false))
            }
            return result.map { Arguments.of(*it.toTypedArray()) }.stream()
        }
    }
}
