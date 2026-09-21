package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.records2.predicate.model.EmptyPredicate
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.ValuePredicate

object DbPredicateUtils {

    /**
     * Whether a predicate asks whether an attribute holds nothing.
     *
     * `empty(att)` and `eq(att, null)` are the same question in two spellings, and every place that
     * answers it has to answer both alike - a column the table does not have, a path through an
     * association, and the degraded forms of that path are four such places today. Kept in one
     * function so a third spelling is added once rather than found missing from one of them.
     *
     * Lives in `service` because both sides of the call already depend on this package; putting it
     * with the callers in `records.dao.query` would have the lower layer importing the upper one.
     */
    fun isEmptinessCheck(predicate: Predicate): Boolean {
        if (predicate is EmptyPredicate) {
            return true
        }
        return predicate is ValuePredicate &&
            predicate.getType() == ValuePredicate.Type.EQ &&
            predicate.getValue().isNull()
    }
}
