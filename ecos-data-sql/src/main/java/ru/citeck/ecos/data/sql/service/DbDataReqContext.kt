package ru.citeck.ecos.data.sql.service

import ru.citeck.ecos.model.lib.type.dto.TypePermsPolicy

object DbDataReqContext {

    val permsPolicy: ThreadLocal<TypePermsPolicy> = ThreadLocal()
    val withoutModifiedMeta: ThreadLocal<Boolean> = ThreadLocal.withInitial { false }

    fun getPermsPolicy(orElse: TypePermsPolicy): TypePermsPolicy {
        return permsPolicy.get() ?: orElse
    }

    inline fun <T> doWithPermsPolicy(permsPolicy: TypePermsPolicy?, action: () -> T): T {
        val prevPolicy = this.permsPolicy.get()
        if (permsPolicy == null) {
            this.permsPolicy.remove()
        } else {
            this.permsPolicy.set(permsPolicy)
        }
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
