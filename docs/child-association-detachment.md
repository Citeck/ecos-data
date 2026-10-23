# Detachable child associations

An `ASSOC` attribute with `config.child: true` maintains the child's `_parent` and
`_parentAtt`. By default, removing a child from the parent's attribute deletes the
child, and deleting the parent cascades to its children.

Set `config.allowChildDetach: true` to keep children when they lose their parent:

```yaml
- id: subtasks
  type: ASSOC
  multiple: true
  config:
    child: true
    allowChildDetach: true
    typeRef: emodel/type@ept-issue-subtask
```

With this configuration, removing a link from the parent's attribute (including
`att_rem_` operations and full attribute replacement) preserves the removed child
and clears both `_parent` and `_parentAtt`. Deleting the parent also detaches its
children instead of deleting them. Each retained child's own descendants remain
attached to it. Moving a child to another parent keeps the existing reparenting
behavior: the old parent's link is removed and the new parent becomes `_parent`.

The flag does not change child creation, the single-parent hierarchy, or recursive
parent checks. It has no effect on ordinary associations with `child: false`.
Omitting it or setting it to `false` preserves the existing cascade behavior.
The current attribute model of the actual parent record determines the policy,
including changes to the flag on existing links and types sharing one DAO.

Load this configuration only after the record-owning service uses an ecos-data
version that implements the flag. Older versions ignore `allowChildDetach` and
still delete children for a `child: true` association. A type artifact change by
itself does not upgrade that service's storage library.

Detachment uses the normal record mutation path. It updates permissions and emits
change events; after detachment the child's normal rules for a record without a
parent apply. Deleting a parent releases its children in the same system context
used by cascade deletion, so permission to delete each child is not required.
An explicit deletion of the child still deletes it.

The policy applies to live child associations. Historical links parked by an
attribute conversion retain the existing column-migration lifecycle.

`DbRecordsChildDetachTest` exercises the behavior through the Records API on both
the PostgreSQL and in-memory backends, alongside the existing child and migration
regressions.
