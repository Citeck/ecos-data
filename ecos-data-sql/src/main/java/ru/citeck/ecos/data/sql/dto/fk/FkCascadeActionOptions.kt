package ru.citeck.ecos.data.sql.dto.fk

enum class FkCascadeActionOptions {
    CASCADE,
    SET_NULL,
    SET_DEFAULT,
    RESTRICT,
    NO_ACTION
}
