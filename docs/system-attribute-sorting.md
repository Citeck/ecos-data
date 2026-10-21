# Sorting stored system attributes

The Records query interface uses public attribute names. PostgreSQL stores some of
those values in columns with different names. `DbFindQueryContext.registerSelectAtt`
resolves direct attribute names before building an ordering or grouping.

| Public attributes | Stored column |
| --- | --- |
| `_type` | `__type` |
| `_workspace` | `__workspace` |
| `id`, `_id`, `_str`, `_localId` | `__ext_id` |
| `_created`, `cm:created` | `__created` |
| `_modified`, `cm:modified` | `__modified` |
| `_creator` | `__creator` |
| `_modifier` | `__modifier` |
| `_status` | `__status` |
| `_name`, `_disp` | `__name` |

The direct selection map extends the existing common mapping. Dotted association
paths keep their existing resolution. Predicate conversion remains separate: type
references, workspace scopes and global identifiers require their own value
handling, so adding a sort alias must not reinterpret filter values.
Expression parsing retains its existing attribute support.

Type and workspace values are stored reference IDs. Ordering them groups records
by the stored reference, as with other reference columns; it does not promise
alphabetical ordering of translated display names. Add an explicit secondary sort
when stable ordering within one type/workspace is needed.

This mapping concerns stored system values. Computed attributes without a stored
column (permissions, content descriptors, paths, etc.) do not become sortable by
adding them here. Lazy optional columns such as `__name` must exist before SQL
can order by them; this change does not alter optional-column migration behavior.

`DbRecordsSortTest` runs through the Records API on both PostgreSQL and the
in-memory backend. It covers type/workspace groups in both directions, a secondary
identifier sort, identity aliases and canonical/legacy audit-date aliases.
