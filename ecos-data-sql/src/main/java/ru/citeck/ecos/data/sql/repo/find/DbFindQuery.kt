package ru.citeck.ecos.data.sql.repo.find

import ru.citeck.ecos.data.sql.service.assocs.AssocJoin
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbFindQuery(
    val expressions: Map<String, ExpressionToken>,
    val assocJoins: List<AssocJoin>,
    val assocTableJoins: List<AssocTableJoin>,
    val predicate: Predicate,
    val withDeleted: Boolean,
    val sortBy: List<DbFindSort>,
    val groupBy: List<String>
) {

    companion object {

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbFindQuery {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    fun copy(): Builder {
        return Builder(this)
    }

    class Builder() {

        var predicate: Predicate = Predicates.alwaysTrue()
        var withDeleted: Boolean = false
        var sortBy: List<DbFindSort> = emptyList()
        var groupBy: List<String> = emptyList()

        var expressions: MutableMap<String, ExpressionToken> = HashMap()
        var assocJoins: MutableList<AssocJoin> = ArrayList()
        var assocTableJoins: MutableList<AssocTableJoin> = ArrayList()

        constructor(base: DbFindQuery) : this() {
            this.predicate = base.predicate.copy()
            this.withDeleted = base.withDeleted
            this.sortBy = base.sortBy
            this.groupBy = base.groupBy
            this.expressions = HashMap(base.expressions)
            this.assocJoins = ArrayList(base.assocJoins)
            this.assocTableJoins = ArrayList(base.assocTableJoins)
        }

        fun withPredicate(predicate: Predicate?): Builder {
            this.predicate = predicate ?: Predicates.alwaysTrue()
            return this
        }

        fun withDeleted(withDeleted: Boolean?): Builder {
            this.withDeleted = withDeleted ?: false
            return this
        }

        fun withSortBy(sortBy: List<DbFindSort>?): Builder {
            this.sortBy = sortBy ?: ArrayList()
            return this
        }

        fun withGroupBy(groupBy: List<String>?): Builder {
            this.groupBy = groupBy ?: emptyList()
            return this
        }

        fun withExpressions(expressions: Map<String, ExpressionToken>): Builder {
            this.expressions = LinkedHashMap(expressions)
            return this
        }

        fun addExpression(alias: String, token: ExpressionToken): Builder {
            this.expressions[alias] = token
            return this
        }

        fun withAssocJoins(assocJoins: List<AssocJoin>?): Builder {
            this.assocJoins = ArrayList(assocJoins ?: emptyList())
            return this
        }

        fun addAssocJoin(assocJoin: AssocJoin): Builder {
            this.assocJoins.add(assocJoin)
            return this
        }

        fun withAssocTableJoins(assocTableJoins: List<AssocTableJoin>?): Builder {
            this.assocTableJoins = ArrayList(assocTableJoins ?: emptyList())
            return this
        }

        fun addAssocTableJoin(assocTableJoin: AssocTableJoin): Builder {
            this.assocTableJoins.add(assocTableJoin)
            return this
        }

        fun build(): DbFindQuery {
            return DbFindQuery(
                expressions = expressions,
                assocJoins = assocJoins,
                assocTableJoins = assocTableJoins,
                predicate = predicate,
                withDeleted = withDeleted,
                sortBy = sortBy,
                groupBy = groupBy
            )
        }
    }
}
