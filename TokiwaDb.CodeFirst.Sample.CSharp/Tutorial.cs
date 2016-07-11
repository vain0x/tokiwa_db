using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

// ここでは、常磐DBを C# から使用する方法を説明します。
// Here we describe how to use Tokiwa DB in C#.

// 初めに、 TokiwaDb.Core.dll と TokiwaDb.CodeFirst.dll への参照をプロジェクトに追加する必要があります。
// First of all, you need to add references to TokiwaDb.Core.dll and TokiwaDb.CodeFirst.dll.

namespace TokiwaDb.CodeFirst.Sample.CSharp
{
    // データベースを作成するために、 TokiwaDb.CodeFirst.Model クラスを継承した「モデル」クラスを定義します。
    // これらのインスタンスはレコードを表現します。
    // ここでは例として、モデルクラス Person と Song を定義します。
    // To create database, you must define "model" classes, inheriting TokiwaDb.CodeFirst.Model, whose instances represent records. For example:
    public class Person
        : Model
    {
        // モデルクラスのセッター (setter) を持つプロパティは、レコードのフィールドを表すとみなされます。
        // これらのプロパティの型は long, double, DateTime, string, byte[] のいずれかでなければなりません。
        // メモ: int, char[] などは使えません。
        // Properties with setter of model classes are considered to represent fields of a record.
        // These properties must be of long, double, DateTime, string or byte[].
        // Note: int, char[], etc. are NOT allowed.
        public string Name { get; set; }
        public long Age { get; set; }

        // 必要に応じて、その他の定義を含めてもかまいません。
        // And other definitions if necessary.
        public override string ToString()
        {
            return string.Format("{0} ({1})", Name, Age);
        }
    }

    public class Song
        : Model
    {
        public string Title { get; set; }
        public string VocalName { get; set; }
    }

    //-------------------------------------------

    [TestClass]
    public class Tutorial
    {
        public Database OpenDatabase()
        {
            // データベースを作成したり、データベースに接続したりするには、DbConfig を使用します。以下にその手順を説明します。
            // To connect or create a database, use DbConfig. Like this:
            var dbConfig = new DbConfig();

            // モデルクラスをデータベースに登録します。
            // ここでモデルクラスの登録を忘れると、それに対応するテーブルにアクセスしたときに例外が投げられてしまいます。
            // Register model classes to the database.
            // Don't forget to enumerate all of your model classes here;
            // or an exception will be thrown when accessing to any of missing tables.
            dbConfig.AddTable<Song>();

            // 一意性制約が使用できます。
            // メモ: 実際にはハッシュ表の索引が作られますが、それを使用する手段は未実装です。
            // Unique constraints are available.
            // Note: Actually a hashtable index is created for each unique contraints, you can't use it yet.
            dbConfig.AddTable<Person>(UniqueIndex.Of<Person>(p => p.Name));

            // そして、OpenMemory メソッドを呼び出して、インメモリーのデータベースを生成します。
            // Then invoke OpenMemory to create an in-memory database.
            return dbConfig.OpenMemory("sample_db");

            // ディスクベースのデータベースに対しては、代わりに OpenDirectory メソッドを使用してください。
            // これは OpenMemory とは異なり、既存のデータベースを開くことができます。
            // 重要: データベースのディレクトリーがすでに存在し、しかしモデルクラスが異なっている場合、
            //       すべてのテーブルが Drop され、改めて新しいテーブルが作られます。
            // Use OpenDirectory for disk-based one instead.
            // Unlike OpenMemory, OpenDirectory can open an exsiting database.
            // Note: If the database directory exists but model classes have changed,
            //       all tables are dropped and new tables are created again.

            //return dbConfig.OpenDirectory(new System.IO.DirectoryInfo(@"path/to/directory"));
        }

        [TestMethod]
        public void InsertSample()
        {
            // データベースを作成、あるいは接続します。
            // Open (connect) or create the database.
            using (Database db = OpenDatabase())
            {
                // テーブルにアクセスするには、Database.Table<ModelClass> メソッドを使います。
                // 返されるオブジェクトが、ModelClass に対応するテーブルにアクセスする手段を提供します。
                // To access to tables, use Database.Table<ModelClass> method.
                // The returned object provides the way to access to the corresponding table to ModelClass.
                Table<Person> persons = db.Table<Person>();

                // Insert メソッドは、モデルクラスのインスタンスをレコードとしてテーブルに挿入するメソッドです。
                // トランザクションの外側では、この処理は即座に反映されます。
                // メモ: 挿入されるインスタンスの Id プロパティは無効値 (あるいは既定値) でなければなりません。
                // メモ: 一意性制約に違反する場合、例外が投げられます。
                // Insert method inserts a model instance as a record to the table.
                // This effects immediately out of transactions.
                // Note: Id property of the inserted instance must be invalid (or default).
                // Note: This may throw an exception because of uniqueness constraints.
                var person = new Person() { Name = "Miku", Age = 16L };
                Assert.IsTrue(person.Id < 0L);
                persons.Insert(person);

                // Insert メソッドの後、挿入されるインスタンスの Id プロパティの値が、それの Id に設定されます。
                // これはトランザクションの中でも同様です。
                // After the Insert method, Id property of the inserted instance is set to its Id
                // both in or out of transactions.
                Assert.AreEqual(0L, person.Id);

                // 現在の Person テーブルには1個のレコードが含まれていることになります。
                // Now the Person table contains one record.
                Assert.AreEqual(1L, persons.CountAllRecords);
            }
        }

        // 後のサンプルのため、サンプルデータを含むデータベースを作成する関数を定義しておきます。
        // For the later samples, we define a helper function which creates a database with sample data.
        public Database CreateSampleDatabase()
        {
            var db = OpenDatabase();
            var persons = db.Table<Person>();
            persons.Insert(new Person() { Name = "Miku", Age = 16L });
            persons.Insert(new Person() { Name = "Yukari", Age = 18L });

            var songs = db.Table<Song>();
            songs.Insert(new Song() { Title = "Rollin' Girl", VocalName = "Miku" });
            songs.Insert(new Song() { Title = "Sayonara Chainsaw", VocalName = "Yukari" });
            return db;
        }

        [TestMethod]
        public void ItemsSample()
        {
            using (var db = CreateSampleDatabase())
            {
                var persons = db.Table<Person>();
                var songs = db.Table<Song>();

                // Table<M>.Items は削除されていないすべてのインスタンスを IEnumerable<M> として返します。
                // このシーケンスは、レコードの読み込みとインスタンスの生成を必要に応じて行います。
                // Table<M>.Items returns all "live" instances as an IEnumerable<M>.
                // The sequence reads and constructs model instances on demand.
                IEnumerable<Person> items = persons.Items;

                // LINQ to Object が使用できます。
                // You can use "LINQ to Object".
                Assert.AreEqual("Miku", items.ElementAt(0).Name);

                // クエリー式は、複雑なクエリーを書くときの助けになります。
                // Query expressions help you to write complex queries.
                var queryResult =
                    from person in persons.Items
                    join song in songs.Items on person.Name equals song.VocalName
                    where person.Age >= 18L
                    select new { Name = person.Name, Title = song.Title, Age = person.Age };
                Assert.AreEqual(1, queryResult.Count());
                Assert.AreEqual(new { Name = "Yukari", Title = "Sayonara Chainsaw", Age = 18L }, queryResult.First());
            }
        }

        [TestMethod]
        public void RemoveSample()
        {
            using (var db = CreateSampleDatabase())
            {
                var persons = db.Table<Person>();

                // 後で「現時点のデータベース」を参照するために、今のリビジョン番号を記録しておきます。
                // Save the current revision number of the database
                // to access to the database with the current state later.
                var savedRevisionId = db.CurrentRevisionId;

                // Remove メソッドは、指定された Id を持つレコードをテーブルから除去します。
                // Remove method removes the record with the given Id from the table.
                var miku = persons.Items.First();
                Assert.AreEqual("Miku", miku.Name);
                persons.Remove(miku.Id);

                // 現在の Person テーブルには、Miku という名前のデータがなくなっていることになります。
                // The Person table no longer contains Miku.
                Assert.IsFalse(persons.Items.Any(p => p.Name == "Miku"));

                // しかし、Remove メソッドは論理削除を行うだけです。
                // AllItems と savedRevisionId を使うことで、Miku のデータを再び得ることができます。
                // AllItems は、削除されたものも含めて、テーブルに含まれるすべてのインスタンスを返します。
                // あるインスタンスがリビジョン t において有効かどうかは、IsLiveAt(t) の真偽値で判断します。
                // The Remove method, however, performs logical deletion.
                // You can get Miku again by using AllItems and savedRevisionId.
                // AllItems returns all instances in the table including removed ones.
                // Each of those is valid at the revision t if and only if IsLiveAt(t) returns true.
                var items = persons.AllItems.Where(p => p.IsLiveAt(savedRevisionId));
                Assert.AreEqual(miku.ToString(), items.First().ToString());
            }
        }

        [TestMethod]
        public void TransactionSample()
        {
            using (var db = CreateSampleDatabase())
            {
                var persons = db.Table<Person>();

                // Database.Transaction はトランザクションオブジェクトを返します。
                // これはデータベースごとに一意なオブジェクトです。
                // 始め、トランザクションは開始されていないので、前述のとおりすべての操作 (Insert, Remove) は即座に反映されます。
                // Database.Transaction returns the transaction object.
                // It's singleton for each database.
                // At first no transactions are beginning,
                // so all operations (Insert, Remove) affects immediately as above.
                var transaction = db.Transaction;

                try
                {
                    // Transaction.Begin は新しいトランザクションを開始します。
                    // メモ: ネストされたトランザクションを開始することも可能です。
                    // Transaction.Begin begins new transaction.
                    // Note: You can also begin nested transactions.
                    transaction.Begin();

                    // 例として、いろいろ操作を行います。
                    // Do operations for example...
                    {
                        var rin = new Person() { Name = "Rin", Age = 14L };
                        persons.Insert(rin);
                        Assert.AreEqual(2L, rin.Id);

                        persons.Remove(persons.Items.First().Id);

                        // トランザクション中の操作は、すぐには反映されません。
                        // Operations during a transaction don't affect immediately.
                    }

                    // Transaction.Commit は現在のトランザクションを終了させます。
                    // そのトランザクション中に登録されたすべての操作は、ここで実行されます (ネストされたトランザクションでない場合)。
                    // Transaction.Commit ends the current transaction.
                    // All operations registered during the transaction are performed now unless the transaction is nested.
                    transaction.Commit();
                }
                catch (Exception)
                {
                    // Transaction.Rollback も現在のトランザクションを終了させます。
                    // トランザクション中に登録されたすべての操作は破棄されます。
                    // Transaction.Rollback method also ends the current transaction.
                    // All operations registered during the transaction are just discarded.
                    transaction.Rollback();
                    throw;
                }
            }
        }
    }
}
