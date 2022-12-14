package ru.citeck.ecos.data.sql.content

interface EcosContentService {

    fun uploadContent(storage: String, data: EcosContentUploadData): EcosContentDbData

    fun getContent(id: Long): EcosContentDbData?
}
