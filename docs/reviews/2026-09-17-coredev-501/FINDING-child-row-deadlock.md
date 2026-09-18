# Находка: дедлок на строке ребёнка — цикл не исчез, а переехал

Дата: 2026-09-17. Найдено собственным ревью правки `2e6d9d7` (перестановка заявки в пути arrival).
**ИСПРАВЛЕНО** в этой же ветке; тесты в наборе (см. «Что закрыто» внизу).

## Суть

Дочерняя ассоциация вводит **третий** ресурс — строку самого ребёнка, куда пишется `_parent`.

Порядок захвата у миграции до правки (и в восстановлении, и в arrival, и до `2e6d9d7`, и после):

```
строка ребёнка (adoptArrivedChildren → updateParentRefOfChildren)
  → связь родителя (createAssocs)
    → строка родителя (claimTheRecord)
```

Порядок у обычной пользовательской мутации, добавляющей того же ребёнка:

```
связь родителя (setMutationAtts → createAssocs)
  → строка родителя (dataService.save)
    → строка ребёнка (processAssocsAfterMutation → updateParentRefOfChildren)
```

Первые два ресурса взяты в противоположном порядке → цикл → `ERROR: deadlock detected`.

Перестановка заявки за связи (`84673d0`, `2e6d9d7`) этот случай не закрывала: она выровняла пару
«связь ↔ строка родителя», а пара «строка ребёнка ↔ связь родителя» осталась перевёрнутой.

## Починка

`adoptArrivedChildren` разделён на две половины:

- `childrenThisRecordMayTake` — **только чтение** (`_parent`, `_parentAtt`, отсутствие records dao,
  рекурсивная ссылка, неадресуемая ссылка). Вызывается до `createAssocs`, как и раньше, потому что
  ребёнка, которого обычный путь записи отверг бы, нельзя делать связью вообще — но чтобы это
  решить, писать не нужно;
- `takeChildren` — записи `_parent`/`_parentAtt`, **после** `createAssocs`.

Порядок миграции стал `связь родителя → строка ребёнка → строка родителя`, у пользователя он
`связь родителя → строка родителя → строка ребёнка`. Первый общий ресурс при совпадающем target —
связь родителя, и она линеаризует обе транзакции до того, как кто-либо коснётся строки ребёнка.

Цена: отказ ребёнка теперь приходит, когда связь уже создана. `takeChildren` возвращает
`AdoptedChildren.refused`, и оба вызывающих снимают ровно эти связи (`removeAssocs`) — в
`createArrivedAssocs` заодно пересчитывается `WHOLE`/`PART`/`NONE`, в `restoreAssocsOfWindow` такие
цели убираются из `addedByChild` и из `recreated` до `restoreAssocsMeta`.

Случай «records dao не поднят» остался **до** любой записи: проверка живёт в половине-читателе, так
что вызывающий по-прежнему узнаёт об этом, ничего не написав.

## Второй дедлок, который открылся этой перестановкой — DDL

Перестановка в пути **arrival** сначала дала регрессию: тест
`aUserAddingTheSameChildInsideAnArrivalDeadlocksWithNothingTest` был зелёным до правки и красным
после, с другим циклом:

```
Process 84 waits for ShareLock on transaction 499; blocked by process 85.
Process 85 waits for AccessExclusiveLock on relation 16543; blocked by process 84.
```

`AccessExclusiveLock` — это DDL. В логе видно, кто его просил:

```
[user-mutation] ALTER TABLE "records-test-schema"."test-records-table" ADD COLUMN "_parent" BIGINT
[user-mutation] CREATE INDEX ON "records-test-schema"."test-records-table" ("_parent")
[user-mutation] ALTER TABLE "records-test-schema"."test-records-table" ADD COLUMN "_parentAtt" BIGINT
```

`_parent`/`_parentAtt` — колонки из `DbRecord.OPTIONAL_COLUMNS`: они добавляются лениво, первой же
мутацией, которая даёт записи родителя. Таблица, где ни один атрибут никогда не был child-ассоциацией,
их не имеет. До перестановки этот DDL выполняла сама миграция (в `adoptArrivedChildren`, до первой
блокировки строки), и пользователю он уже не требовался. После перестановки его просит пользователь —
и ждёт `AccessExclusiveLock` за батчем миграции, пока батч ждёт его же строку связи.

Починка — не возвращать DDL внутрь батча (там он ещё хуже: блокировка уровня таблицы, взятая
поверх блокировок строк), а выполнить его вместе с остальным DDL перехода:
`DbShadowColumnTransition.afterTransition` → `addChildBackReferenceColumns()` при `targetChild`.
Идемпотентно, O(1) на таблице, которая уже держала детей, и переносит построение индекса по
`_parent` из мутации случайного пользователя в смену типа, которую делает администратор.

Это улучшение и для выпущенного `55d466c`: там DDL мог выполниться внутри транзакции батча.

## Что закрыто и чем проверено

Два теста в `DbColumnMigrationHandlerTest` (оба зелёные, оба падают на коде до правки):

- `aUserAddingTheSameChildInsideTheRestoreDeadlocksWithNothingTest` — путь восстановления;
- `aUserAddingTheSameChildInsideAnArrivalDeadlocksWithNothingTest` — путь arrival.

Красноту подтверждал прогоном: на `dfeb349` первый падает с `ERROR: deadlock detected` в
`restoreAssocsOfWindow`; с уже исправленным `DbShadowColumnTransition`, но старым handler'ом падают
оба, и уже чистым циклом блокировок строк (`ShareLock on transaction`, без `AccessExclusiveLock`) —
то есть тесты ловят именно перестановку, а не DDL.

Полный набор после правки: `ecos-data-sql` 140/0/0, `ecos-data-sql-pg` 936/0 (1 skipped),
`ecos-data-inmem` 1814/0 (85 skipped). База до правки — 140 / 934 (1) / 1812 (83).
