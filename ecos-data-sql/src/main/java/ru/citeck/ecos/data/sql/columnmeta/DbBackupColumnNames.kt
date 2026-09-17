package ru.citeck.ecos.data.sql.columnmeta

/**
 * What a backup column is called.
 *
 * **The registry is the source of truth; the name is readable best-effort.** Nothing in the
 * migration machinery may parse a column name to learn what it holds - it reads the
 * `ed_column_meta` row. The name exists so that a DBA looking at the table with `\d` can see what
 * happened without a join.
 *
 * Two leading underscores make a collision with a real attribute impossible: an attribute id cannot
 * begin with `_`, which `DbEcosModelService.mapAttToColumn` refuses as a reserved prefix.
 */
object DbBackupColumnNames {

    const val PREFIX = "__backup_"

    /**
     * The form used when the readable one does not fit.
     *
     * Its last segment is a number, but that alone does not separate it from a readable name: once
     * a collision counter is appended, a readable name also ends in a numeric segment (e.g.
     * `__backup_regNum_text_2`). What actually keeps the two forms disjoint is narrower - a readable
     * name could only collide with this one if an attribute were literally named `column` *and* its
     * type were spelled as a number, and no [ru.citeck.ecos.model.lib.attributes.dto.AttributeType]
     * and no [ru.citeck.ecos.data.sql.dto.DbColumnType] is - pinned by
     * `DbBackupColumnNamesTest.theNumberedFallbackCannotBeMistakenForAReadableNameTest`. Either way,
     * nothing may infer a column's contents from its name: the registry row is the source of truth
     *.
     */
    private const val FALLBACK_PREFIX = PREFIX + "column_"

    private const val MULTIPLE_SUFFIX = "_multiple"

    /**
     * @param attId the id of the attribute the column used to hold - the *original* id, which for a
     *              backup of a backup is still the original one.
     * @param attType what the column holds, as the registry records it.
     * @param multiple whether the column is an array.
     * @param maxBytes the backend's column name limit, from `DbSchemaDao.getMaxColumnNameBytes()`.
     *                 Measured in **bytes**, not characters: a Cyrillic attribute id costs two bytes
     *                 per character on PostgreSQL.
     * @param taken every column name already present in the table - both the physical columns and
     *              any name this transition has already decided to use. A name is never handed out
     *              twice.
     */
    @JvmStatic
    fun build(
        attId: String,
        attType: DbColumnSemanticType,
        multiple: Boolean,
        maxBytes: Int,
        taken: Set<String>
    ): String {
        val readable = PREFIX + attId + "_" + attType.asString() + if (multiple) MULTIPLE_SUFFIX else ""
        if (fits(readable, maxBytes)) {
            if (!taken.contains(readable)) {
                return readable
            }
            // The same attribute backed up twice at the same type. Counting from 2 keeps the first
            // backup's name unchanged, which matters because it is already in the registry and in
            // ed_associations_backup rows.
            var counter = 2
            while (counter < Int.MAX_VALUE) {
                val candidate = readable + "_" + counter
                if (fits(candidate, maxBytes) && !taken.contains(candidate)) {
                    return candidate
                }
                if (!fits(candidate, maxBytes)) {
                    // No room for a counter. Truncating the readable part instead would produce a
                    // name that looks readable and is not, and could collide with a different
                    // attribute whose id shares a prefix.
                    break
                }
                counter++
            }
        }
        return fallback(attId, maxBytes, taken)
    }

    /**
     * True for both forms, and **the only thing that keeps a backup column out of reads**.
     *
     * The registry flag is the criterion and the prefix its backstop; the
     * implementation is the other way round, and deliberately so. `DbDataServiceImpl`'s table
     * context filters its visible column list on this predicate alone and consults no registry row
     * (see `DbTableContext.getColumns`). The two are equivalent in what they exclude - the prefix
     * cannot collide with an attribute, because an attribute id may not begin with `_`
     * (`DbEcosModelService.mapAttToColumn` refuses it), and every backup this machinery makes
     * carries it - and the
     * name is the cheaper of the two by a whole query per read: the alternative would make every
     * read of every table depend on `ed_column_meta` being loaded and current.
     *
     * The consequence to keep in mind is that this is a **name-based** rule: a column called
     * `__backup_...` is invisible to reads whether or not the registry has ever heard of it. That
     * is what makes an installation seeded before the registry existed safe, and it is also why no
     * future naming scheme for backups may drop the prefix.
     */
    @JvmStatic
    fun isBackupName(name: String): Boolean {
        return name.startsWith(PREFIX)
    }

    /**
     * The spec defines no fallback beyond this one, so a `maxBytes` too small even for
     * `__backup_column_<N>` is not a case to degrade further from - there is nothing sensible left
     * to hand back. Failing loudly here, with the attribute id and limit named, is far more useful
     * than emitting an oversized name and letting the DDL reject it later with a message that does
     * not mention this attribute at all.
     */
    private fun fallback(attId: String, maxBytes: Int, taken: Set<String>): String {
        var counter = 1
        while (true) {
            val candidate = FALLBACK_PREFIX + counter
            if (!fits(candidate, maxBytes)) {
                error(
                    "Cannot build a backup column name for attribute '$attId': even the numbered " +
                        "fallback '$candidate' does not fit within the $maxBytes byte column name limit."
                )
            }
            if (!taken.contains(candidate)) {
                return candidate
            }
            counter++
        }
    }

    private fun fits(name: String, maxBytes: Int): Boolean {
        return name.toByteArray(Charsets.UTF_8).size <= maxBytes
    }
}
