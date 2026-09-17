package ru.citeck.ecos.data.sql.columnmeta

import ru.citeck.ecos.data.sql.dto.DbColumnType
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

/**
 * Spec section 5: which class of conversion a change of an attribute's type falls into.
 *
 * Deliberately pure and free of SQL. The class decides *what the migration promises the user* - and
 * that promise cannot depend on which database is underneath, because the row-by-row transfer is a
 * universal fallback that every backend can run. A backend that happens to be able to
 * express a pair as a single cast is an optimisation, never a change of class.
 *
 * The rules are applied top to bottom and **the first match wins**. Reordering them changes the
 * answers.
 */
object DbColumnConversions {

    private val G_STR = setOf(AttributeType.TEXT, AttributeType.MLTEXT, AttributeType.OPTIONS)

    /**
     * `G_STR` without [AttributeType.MLTEXT] - the members that really are interchangeable bare
     * strings. See [isSameStoredForm] for why MLTEXT cannot be one of them.
     */
    private val G_STR_PLAIN = setOf(AttributeType.TEXT, AttributeType.OPTIONS)

    private val G_ASSOC = setOf(
        AttributeType.ASSOC,
        AttributeType.PERSON,
        AttributeType.AUTHORITY,
        AttributeType.AUTHORITY_GROUP
    )

    /**
     * @param source what the column registry says the column holds now - either a model type or,
     *               for a column first met on an installation that predates the registry, only its
     *               physical shape.
     * @param sourceMultiple whether the column is physically an array now.
     * @param target the attribute type the model asks for.
     * @param targetMultiple whether the model asks for an array.
     */
    @JvmStatic
    fun classify(
        source: DbColumnSemanticType,
        sourceMultiple: Boolean,
        target: AttributeType,
        targetMultiple: Boolean
    ): DbConversionClass {
        val byType = when (source) {
            is DbColumnSemanticType.Model -> classifyTypes(source.attType, target)
            // the model type was never recorded, so every attribute type of that physical
            // shape is a candidate and the most cautious of them decides. Being too careful here
            // costs an untransferred column with its data intact in the backup; being too bold
            // costs values converted from a type they never had.
            is DbColumnSemanticType.Raw -> candidatesOf(source.columnType)
                .maxOfOrNull { classifyTypes(it, target) }
                ?: DbConversionClass.NONE
        }
        return worstOf(byType, classifyMultiple(sourceMultiple, targetMultiple, target))
    }

    /**
     * [DbConversionClass.NONE] is the only class with nothing to do in the background.
     */
    @JvmStatic
    fun isTransferable(cls: DbConversionClass): Boolean {
        return cls != DbConversionClass.NONE
    }

    /**
     * Whether the stored bytes mean exactly the same thing to both types, so that changing the
     * model from one to the other needs no value transfer at all.
     *
     * True of `TEXT <-> OPTIONS` and of any move between assoc-like types, and deliberately
     * narrower than [DbConversionClass.SAFE]: `TEXT/OPTIONS -> MLTEXT` is safe *and* needs an
     * `UPDATE`, because the column type does not move but a bare string is not an `MLText` object
     * and `DbRecordsQueryDao` stops finding it by `EQ` once the model says it is one. Treating that
     * pair as free is the exact defect this map exists to correct.
     *
     * Only meaningful when the physical column definition itself does not change - when it does,
     * the backend's `ALTER ... TYPE ... USING` is what rewrites the values.
     *
     * A source the registry could only describe physically ([DbColumnSemanticType.Raw]) is never
     * free: nothing ever recorded what those bytes mean.
     */
    @JvmStatic
    fun isSameStoredForm(source: DbColumnSemanticType, target: AttributeType): Boolean {
        val from = (source as? DbColumnSemanticType.Model)?.attType ?: return false
        if (from == target) {
            return true
        }
        if (from in G_STR_PLAIN && target in G_STR_PLAIN) {
            return true
        }
        return from in G_ASSOC && target in G_ASSOC
    }

    private fun candidatesOf(columnType: DbColumnType): List<AttributeType> {
        return AttributeType.entries.filter { DbAttTypeColumns.getColumnType(it) == columnType }
    }

    private fun worstOf(a: DbConversionClass, b: DbConversionClass): DbConversionClass {
        return if (a.ordinal >= b.ordinal) a else b
    }

    /**
     * Multiplicity, orthogonal to the type: widening a value into a one-element array loses nothing,
     * narrowing keeps the first element and leaves the rest in the backup.
     *
     * Two targets have no multiplicity dimension at all, and for them both directions are a no-op.
     * A JSON column holds whatever shape it is given, so the array lives inside the document. A
     * CONTENT column is a single `ed_content` id whatever the model's flag says
     * ([DbAttTypeColumns.isMultiple]), so it is the same column either way and a "narrowing" has
     * nothing to narrow - calling it lossy made this map contradict the machinery it describes,
     * which never sees a change of any kind there.
     */
    private fun classifyMultiple(
        sourceMultiple: Boolean,
        targetMultiple: Boolean,
        target: AttributeType
    ): DbConversionClass {
        if (sourceMultiple == targetMultiple ||
            target == AttributeType.JSON ||
            !DbAttTypeColumns.isMultiple(target, true)
        ) {
            return DbConversionClass.SAFE
        }
        return if (targetMultiple) DbConversionClass.SAFE else DbConversionClass.LOSSY
    }

    private fun classifyTypes(from: AttributeType, to: AttributeType): DbConversionClass {
        // CONTENT first, because it is the one unconditional case: checking it here makes that
        // visible, rather than something a reader has to prove by checking that CONTENT is absent
        // from every set used below
        if (from == AttributeType.CONTENT || to == AttributeType.CONTENT) {
            return if (from == to) DbConversionClass.SAFE else DbConversionClass.NONE
        }
        // an MLText losing its locales
        if (from == AttributeType.MLTEXT && (to == AttributeType.TEXT || to == AttributeType.OPTIONS)) {
            return DbConversionClass.LOSSY
        }
        if (from == AttributeType.MLTEXT && to == AttributeType.JSON) {
            return DbConversionClass.LOSSY
        }
        // arbitrary JSON need not be a valid MLText
        if (from == AttributeType.JSON && to == AttributeType.MLTEXT) {
            return DbConversionClass.LOSSY
        }
        // a bare string becoming an MLText object
        if ((from == AttributeType.TEXT || from == AttributeType.OPTIONS) && to == AttributeType.MLTEXT) {
            return DbConversionClass.SAFE
        }
        // inside the string-like group, and inside the assoc-like group
        if (from in G_STR && to in G_STR) {
            return DbConversionClass.SAFE
        }
        if (from in G_ASSOC && to in G_ASSOC) {
            return DbConversionClass.SAFE
        }
        // between an association and a plain reference: the source of truth moves
        if (from in G_ASSOC && to == AttributeType.ENTITY_REF) {
            return DbConversionClass.LOSSY
        }
        if (from == AttributeType.ENTITY_REF && to in G_ASSOC) {
            return DbConversionClass.LOSSY
        }
        // references and strings, both ways
        if (from in G_ASSOC && to in G_STR) {
            return DbConversionClass.LOSSY
        }
        if (from == AttributeType.ENTITY_REF && to in G_STR) {
            return DbConversionClass.LOSSY
        }
        if (from in G_STR && to in G_ASSOC) {
            return DbConversionClass.LOSSY
        }
        if (from in G_STR && to == AttributeType.ENTITY_REF) {
            return DbConversionClass.LOSSY
        }
        // a scalar rendered into a string
        if (from in SAFE_TO_STRING && to in G_STR) {
            return DbConversionClass.SAFE
        }
        // a string into JSON, which it need not be valid as
        if (from in G_STR && to == AttributeType.JSON) {
            return DbConversionClass.LOSSY
        }
        // a string parsed back into a scalar
        if (from in G_STR && to in PARSEABLE_FROM_STRING) {
            return DbConversionClass.LOSSY
        }
        // strings and raw bytes, both ways
        if (from in G_STR && to == AttributeType.BINARY) {
            return DbConversionClass.SAFE
        }
        if (from == AttributeType.BINARY && to in G_STR) {
            return DbConversionClass.LOSSY
        }
        // a date widened to a timestamp, and the narrowing back
        if (from == AttributeType.DATE && to == AttributeType.DATETIME) {
            return DbConversionClass.SAFE
        }
        if (from == AttributeType.DATETIME && to == AttributeType.DATE) {
            return DbConversionClass.LOSSY
        }
        // an unchanged type is always safe, whatever group it is in
        if (from == to) {
            return DbConversionClass.SAFE
        }
        // nothing worth transferring, which is also the deliberate answer for a scalar wrapped
        // in JSON
        return DbConversionClass.NONE
    }

    private val SAFE_TO_STRING = setOf(
        AttributeType.NUMBER,
        AttributeType.BOOLEAN,
        AttributeType.JSON,
        AttributeType.DATE,
        AttributeType.DATETIME
    )

    private val PARSEABLE_FROM_STRING = setOf(
        AttributeType.NUMBER,
        AttributeType.BOOLEAN,
        AttributeType.DATE,
        AttributeType.DATETIME
    )
}
