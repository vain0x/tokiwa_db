# 常磐DB (Tokiwa DB)
常磐DBは、 **半永続** なリレーショナルデータベース。

(Tokiwa DB is a *[partially persistent](https://en.wikipedia.org/wiki/Persistent_data_structure#Partially_persistent)* relational database.)

## 半永続性 (Partially persistent)
常磐DBのデータベースは過去のすべてのバージョンを記録しています。データベース内にはリビジョン番号という数値があります。これは 0 から始まり、新しいテーブルやレコードを作成したり、レコードを削除したりといった操作が行われるごとに増加します。「破壊的変更」操作を行った後でも、データベース内には過去のバージョンが残っているため、古いリビジョン番号を用いてデータベースにアクセスすることで、過去のバージョンを参照することが可能です。

ちなみに半永続性の「半」とは、バージョンの「分岐」を作れないことを意味しています。例えば、Git は「永続性」のデータベースの一種ですから、 **半** 永続性はブランチのない Git のようなものです。

(The database of Tokiwa DB records all old versions. It has a number called revision number. The number starts with 0 and increases whenever an operation such as creating a table or record or deleting records is performed in the database. Even after destructive operations, the old versions are recorded, so it enable you to refer the old versions by accessing to the database with old revision numbers.

Incidentally "partially" means you can't *branch* versions. *Partially persistent* database is similar to Git without branches.)

## 利点 (Advantages)
#### 論理削除の容易さ (Easy logical deletion)
常磐DBに物理削除はありません。削除されるレコードは、単に「そのリビジョンにおいて削除された」という印がつけられるだけです。

(There's no hard deletion in Tokiwa DB. Removed records are just marked as removed at that revision.)

#### 自然なID(Natural IDs)
テーブルからレコードが「削除」されることはないので、それらは常にID順で格納されます。言い換えると、テーブルはレコードの配列だということです。そのため、

- IDを指定してレコードを取得する操作は高々定数時間ででき、
- レコードのIDのための8バイトを節約することができます。

(Records in a table are ordered by their IDs because they are never deleted. In other words, a table is an array of records. This means:

- fetching a record by its ID takes at most constant time; and
- we can save 8 bytes of ID for each record.)

#### ロック不要の読み込み(No Locks for Read)
レコードやストレージから読み込みを行う際、それらのデータは不変であるため、ロックを必要としません。(Reading records or storage don't need lock because those data are immutable.)

#### 自動単体テストとの親和性 (Affinity wiht automated unit tests)
常磐DBでは、データベースをディレクトリー内だけでなくメモリー内にも生成できます。インメモリーのデータベースはディスクベースのものより基本的に高速なので、自動単体テストに役立ちます。

(Tokiwa DB can create a database not only in a directory but also in memory. In-memory databases are useful for automated unit tests because they are usually faster than disk-based ones.)

#### コードファースト(Code-First)
常磐DBは「コードファースト」という便利な機能に対応しています。すなわち、C# や F# などの言語でコードを書くだけで、データベースの生成と構成を行うことが可能です。(Tokiwa DB supports useful *code-first* approach. That is, you can create and configure databases just by writing C#/F# code.)

## 使いかた (Usage)
- [C# でのサンプル (Sample in C#)](TokiwaDb.CodeFirst.Sample.CSharp)
- [VB.NET でのサンプル (Sample in VB.NET)](TokiwaDb.CodeFirst.Sample.VisualBasic)

## ライセンス (License)
パブリックドメイン (Public domain)
