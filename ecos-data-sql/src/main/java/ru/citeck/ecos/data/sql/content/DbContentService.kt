package ru.citeck.ecos.data.sql.content

interface DbContentService {

    fun uploadContent(storage: String, data: DbContentUploadData): DbEcosContentData

    fun getContent(id: Long): DbEcosContentData?
}
