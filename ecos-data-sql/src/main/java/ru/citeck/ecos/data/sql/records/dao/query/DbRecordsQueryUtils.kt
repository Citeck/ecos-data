package ru.citeck.ecos.data.sql.records.dao.query

import ecos.com.fasterxml.jackson210.databind.node.JsonNodeType
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.records2.predicate.PredicateUtils
import ru.citeck.ecos.records2.predicate.model.*

object DbRecordsQueryUtils {

    fun mapAttributePredicates(
        predicate: Predicate,
        tryToMergeOrPredicates: Boolean,
        mapFunc: (AttributePredicate) -> Predicate
    ): Predicate {
        return mapAttributePredicates(predicate, tryToMergeOrPredicates, false, mapFunc)
    }

    fun mapAttributePredicates(
        predicate: Predicate,
        tryToMergeOrPredicates: Boolean,
        onlyAnd: Boolean,
        mapFunc: (AttributePredicate) -> Predicate
    ): Predicate {
        return if (predicate is AttributePredicate) {
            mapFunc(predicate)
        } else if (predicate is ComposedPredicate) {
            val isAnd = predicate is AndPredicate
            if (onlyAnd && !isAnd) {
                return Predicates.alwaysFalse()
            }
            val mappedPredicates: MutableList<Predicate> = java.util.ArrayList()
            for (pred in predicate.getPredicates()) {
                val mappedPred = mapAttributePredicates(pred, tryToMergeOrPredicates, onlyAnd, mapFunc)
                if (PredicateUtils.isAlwaysTrue(mappedPred)) {
                    if (isAnd) {
                        continue
                    } else {
                        return mappedPred
                    }
                } else if (PredicateUtils.isAlwaysFalse(mappedPred)) {
                    if (isAnd) {
                        return mappedPred
                    } else {
                        continue
                    }
                }
                mappedPredicates.add(mappedPred)
            }
            if (mappedPredicates.isEmpty()) {
                if (isAnd) {
                    Predicates.alwaysTrue()
                } else {
                    Predicates.alwaysFalse()
                }
            } else if (mappedPredicates.size == 1) {
                mappedPredicates[0]
            } else {
                if (isAnd) {
                    Predicates.and(mappedPredicates)
                } else {
                    if (tryToMergeOrPredicates) {
                        joinOrPredicateElements(mappedPredicates)
                    } else {
                        Predicates.or(mappedPredicates)
                    }
                }
            }
        } else if (predicate is NotPredicate) {
            val mapped = mapAttributePredicates(predicate.getPredicate(), tryToMergeOrPredicates, false, mapFunc)
            if (mapped is NotPredicate) {
                mapped.getPredicate()
            } else {
                Predicates.not(mapped)
            }
        } else {
            predicate
        }
    }

    private fun joinOrPredicateElements(predicates: List<Predicate>): Predicate {

        if (predicates.isEmpty()) {
            return Predicates.alwaysFalse()
        } else if (predicates.size == 1) {
            return predicates[0]
        }

        fun getValueElementType(value: DataValue): JsonNodeType {
            return if (value.isArray()) {
                if (value.isEmpty()) {
                    JsonNodeType.NULL
                } else {
                    value[0].value.nodeType
                }
            } else {
                value.value.nodeType
            }
        }

        val firstPred = predicates.first()

        if (firstPred !is ValuePredicate) {
            return Predicates.or(predicates)
        }
        val predType = firstPred.getType()
        if (predType != ValuePredicate.Type.EQ &&
            predType != ValuePredicate.Type.CONTAINS &&
            predType != ValuePredicate.Type.IN
        ) {
            return Predicates.or(predicates)
        }

        val valueElemType = getValueElementType(firstPred.getValue())
        if (valueElemType != JsonNodeType.NUMBER) {
            return Predicates.or(predicates)
        }

        val attribute = firstPred.getAttribute()

        for (i in 1 until predicates.size) {
            val predToTest = predicates[i]
            if (predToTest !is ValuePredicate ||
                predToTest.getAttribute() != attribute ||
                getValueElementType(predToTest.getValue()) != valueElemType
            ) {

                return Predicates.or(predicates)
            }
        }

        fun extractValues(resultList: DataValue, value: DataValue) {
            if (value.isNumber() || value.isTextual()) {
                resultList.add(value)
            } else if (value.isArray()) {
                value.forEach { extractValues(resultList, it) }
            }
        }

        val resultList = DataValue.createArr()
        for (predicate in predicates) {
            if (predicate is ValuePredicate) {
                extractValues(resultList, predicate.getValue())
            }
        }
        if (resultList.isEmpty()) {
            return Predicates.alwaysFalse()
        }
        return ValuePredicate(attribute, ValuePredicate.Type.IN, resultList)
    }
}
