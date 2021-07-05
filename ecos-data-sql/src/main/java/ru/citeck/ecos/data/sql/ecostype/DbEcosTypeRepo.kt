package ru.citeck.ecos.data.sql.ecostype

interface DbEcosTypeRepo {

    fun getTypeInfo(typeId: String): DbEcosTypeInfo?
}
