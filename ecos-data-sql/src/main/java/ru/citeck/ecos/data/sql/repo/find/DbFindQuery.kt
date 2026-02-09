package ru.citeck.ecos.data.sql.repo.find

import ru.citeck.ecos.data.sql.context.DbTableContext
import ru.citeck.ecos.data.sql.service.RawTableJoin
import ru.citeck.ecos.data.sql.service.assocs.AssocJoinWithPredicate
import ru.citeck.ecos.data.sql.service.assocs.AssocTableJoin
import ru.citeck.ecos.data.sql.service.expression.token.ExpressionToken
import ru.citeck.ecos.records2.predicate.model.Predicate
import ru.citeck.ecos.records2.predicate.model.Predicates

class DbFindQuery(
    val expressions: Map<String, ExpressionToken>,
    val assocTableJoins: List<AssocTableJoin>,
    val assocSelectJoins: Map<String, DbTableContext>,
    val assocJoinsWithPredicate: List<AssocJoinWithPredicate>,
    val rawTableJoins: Map<String, RawTableJoin>,
    val predicate: Predicate,
    val sortBy: List<DbFindSort>,
    val groupBy: List<String>,
    val userAuthorities: Set<String>,
    val delegatedAuthorities: List<Pair<Set<Long>, Set<String>>>
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
        var sortBy: List<DbFindSort> = emptyList()
        var groupBy: List<String> = emptyList()

        var expressions: MutableMap<String, ExpressionToken> = HashMap()
        var assocTableJoins: MutableList<AssocTableJoin> = ArrayList()
        var assocSelectJoins: Map<String, DbTableContext> = HashMap()
        var assocJoinWithPredicates: MutableList<AssocJoinWithPredicate> = ArrayList()
        var rawTableJoins: Map<String, RawTableJoin> = emptyMap()
        var userAuthorities: Set<String> = emptySet()
        var delegatedAuthorities: List<Pair<Set<Long>, Set<String>>> = emptyList()

        constructor(base: DbFindQuery) : this() {
            this.predicate = base.predicate.copy()
            this.sortBy = base.sortBy
            this.groupBy = base.groupBy
            this.expressions = HashMap(base.expressions)
            this.assocTableJoins = ArrayList(base.assocTableJoins)
            this.assocSelectJoins = HashMap(base.assocSelectJoins)
            this.assocJoinWithPredicates = ArrayList(base.assocJoinsWithPredicate)
            this.rawTableJoins = base.rawTableJoins
            this.userAuthorities = base.userAuthorities
            this.delegatedAuthorities = base.delegatedAuthorities
        }

        fun withPredicate(predicate: Predicate?): Builder {
            this.predicate = predicate ?: Predicates.alwaysTrue()
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

        fun withAssocTableJoins(assocTableJoins: List<AssocTableJoin>?): Builder {
            this.assocTableJoins = ArrayList(assocTableJoins ?: emptyList())
            return this
        }

        fun addAssocTableJoin(assocTableJoin: AssocTableJoin): Builder {
            this.assocTableJoins.add(assocTableJoin)
            return this
        }

        fun withAssocSelectJoins(assocSelectJoins: Map<String, DbTableContext>?): Builder {
            this.assocSelectJoins = HashMap(assocSelectJoins ?: emptyMap())
            return this
        }

        fun withAssocJoinWithPredicates(assocJoinWithPredicates: List<AssocJoinWithPredicate>?): Builder {
            this.assocJoinWithPredicates = ArrayList(assocJoinWithPredicates ?: emptyList())
            return this
        }

        fun addAssocTableJoin(assocJoinWithPredicate: AssocJoinWithPredicate): Builder {
            this.assocJoinWithPredicates.add(assocJoinWithPredicate)
            return this
        }

        fun withRawTableJoins(rawTableJoins: Map<String, RawTableJoin>): Builder {
            this.rawTableJoins = rawTableJoins
            return this
        }

        fun withUserAuthorities(userAuthorities: Set<String>): Builder {
            this.userAuthorities = userAuthorities
            return this
        }

        fun withDelegatedAuthorities(delegatedAuthorities: List<Pair<Set<Long>, Set<String>>>): Builder {
            this.delegatedAuthorities = delegatedAuthorities
            return this
        }

        fun build(): DbFindQuery {
            return DbFindQuery(
                expressions = expressions,
                assocTableJoins = assocTableJoins,
                assocSelectJoins = assocSelectJoins,
                assocJoinsWithPredicate = assocJoinWithPredicates,
                rawTableJoins = rawTableJoins,
                predicate = predicate,
                sortBy = sortBy,
                groupBy = groupBy,
                userAuthorities = userAuthorities,
                delegatedAuthorities = delegatedAuthorities
            )
        }
    }
}
