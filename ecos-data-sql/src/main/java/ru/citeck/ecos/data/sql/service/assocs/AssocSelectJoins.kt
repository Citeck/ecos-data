package ru.citeck.ecos.data.sql.service.assocs

/**
 * The joins a query walks to reach an attribute behind one or more associations, keyed by the path
 * walked so far - `a`, `a.b`, `a.b.c` for a three-step path.
 */
object AssocSelectJoins {

    /**
     * Splits a joined name into the join it belongs to and the column on the table that join lands
     * on - by the longest registered key, because a key is itself a path: with `a` and `a.b`
     * registered, `a.b.c` belongs to `a.b`, not to `a`.
     *
     * Shared by both backends deliberately. They have to give the same answer about which join a
     * name belongs to, and two copies of this walk are two chances to drift apart.
     *
     * A name is split, not carried as a pair, and that is only unambiguous while a column cannot
     * itself be called `b.c`: `a."b.c"` and the path `a.b.c` would otherwise produce the same
     * string, and the longest registered key would answer both with the deeper join. No column can
     * be called that - `DbEcosModelService.VALID_COLUMN_NAME` matches `[\w-_:]+`, so an attribute
     * whose id carries a dot never becomes a column at all: it is skipped where attributes are
     * mapped to columns. Only a column made outside the type model could collide here.
     */
    fun splitColumn(name: String, joinKeys: Set<String>): Pair<String, String>? {
        var dotIdx = name.lastIndexOf('.')
        while (dotIdx > 0) {
            val joinKey = name.substring(0, dotIdx)
            if (joinKeys.contains(joinKey)) {
                return joinKey to name.substring(dotIdx + 1)
            }
            dotIdx = name.lastIndexOf('.', dotIdx - 1)
        }
        return null
    }
}
