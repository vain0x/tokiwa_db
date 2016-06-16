# 常磐DB (Tokiwa DB)
常磐DBは、 **半永続** なリレーショナルデータベース。

(Tokiwa DB is a *[partially persistent](https://en.wikipedia.org/wiki/Persistent_data_structure#Partially_persistent)* relational database.)

## 半永続性 (Partially persistent)
常磐DBのデータベースは過去のすべてのバージョンを記録しています。データベース内にはリビジョン番号という数値があります。これは 0 から始まり、新しいテーブルやレコードを作成したり、レコードを削除したりといった操作が行われるごとに増加します。「破壊的変更」操作を行った後でも、データベース内には過去のバージョンが残っているため、古いリビジョン番号を用いてデータベースにアクセスすることで、過去のバージョンを参照することが可能です。

ちなみに半永続性の「半」とは、バージョンの「分岐」を作れないことを意味しています。例えば、Git は「永続性」のデータベースの一種ですから、 **半** 永続性はブランチのない Git のようなものです。

(The database of Tokiwa DB records all old versions. It has a number called revision number. The number starts with 0 and increases whenever an operation such as creating a table or record or deleting records is performed in the database. Even after destructive operations, the old versions are recorded, so it enable you to refer the old versions by accessing to the database with old revision numbers.

Incidentally "partially" means you can't *branch* versions. *Partially persistent* database is similar to Git without branches.)

### 利点 (Profits)
- 自動単体テストとの親和性が高い。 (Affinity with automated unit tests.)
- 論理削除の取り扱いが容易。 (Easy to program logical deletion.)

## 使いかた (Usage)
[テストケース](TokiwaDb.Core.Test)を参照。

(Please see [test cases](TokiwaDb.Core.Test).)

## ライセンス (License)
パブリックドメイン (Public domain)
