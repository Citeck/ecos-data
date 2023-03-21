package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy

object DbDataReqContext {

    val permsPolicy: ThreadLocal<TypePermsPolicy> = ThreadLocal.withInitial { TypePermsPolicy.DEFAULT }
    val withoutModifiedMeta: ThreadLocal<Boolean> = ThreadLocal.withInitial { false }

    fun getPermsPolicy(vararg defaultPolicies: TypePermsPolicy): TypePermsPolicy {
        var result = permsPolicy.get()
        var defaultIdx = 0
        while (result == TypePermsPolicy.DEFAULT && defaultIdx < defaultPolicies.size) {
            result = defaultPolicies[defaultIdx++]
        }
        if (result == TypePermsPolicy.DEFAULT) {
            result = TypePermsPolicy.OWN
        }
        return result
    }

    inline fun <T> doWithPermsPolicy(permsPolicy: TypePermsPolicy?, action: () -> T): T {
        val prevPolicy = this.permsPolicy.get()
        this.permsPolicy.set(permsPolicy ?: TypePermsPolicy.DEFAULT)
        try {
            return action.invoke()
        } finally {
            if (prevPolicy == null) {
                this.permsPolicy.remove()
            } else {
                this.permsPolicy.set(prevPolicy)
            }
        }
    }

    inline fun <T> doWithoutModifiedMeta(action: () -> T): T {
        val before = withoutModifiedMeta.get()
        withoutModifiedMeta.set(true)
        try {
            return action.invoke()
        } finally {
            withoutModifiedMeta.set(before)
        }
    }
}
