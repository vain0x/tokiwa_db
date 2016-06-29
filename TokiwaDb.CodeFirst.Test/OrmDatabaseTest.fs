namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

type Person() =
  inherit Model<Person>()

  member val Name = "" with get, set
  member val Age = 0L with get, set

type Song() =
  inherit Model<Person>()

  member val Name = "" with get, set
  member val Vocal = "" with get, set

module OrmDatabaseTest =
  let schemas =
    [
      (typeof<Person>, TableSchema.ofModel<Person> ())
      (typeof<Song>, TableSchema.ofModel<Song> ())
    ]

  let testDb () = OrmDatabase(new MemoryDatabase("test_db"), schemas)

  let createTest =
    test {
      let db = testDb ()
      do! db.Name |> assertEquals "test_db"
    }

  let tableTest =
    test {
      let db = testDb ()
      do! (db.Table<Person> ()).Name |> assertEquals "Person"
      let! _ = trap { it (db.Table<IModel>()) }
      return ()
    }

  let reopenTest =
    test {
      let implDb = new MemoryDatabase("test_db")
      let db = OrmDatabase(implDb, schemas)
      db.Table<Person>().Insert(Person(Name = "Miku", Age = 16L))
      // We can reopen the database with the same models.
      let db = OrmDatabase(implDb, schemas)
      do! (db.Table<Person>()).Items
        |> Seq.map (fun p -> p.Name)
        |> Seq.toList
        |> assertEquals ["Miku"]
      // Opening with different models, all tables are dropped.
      let anotherSchemas = [schemas |> List.head]
      let db = OrmDatabase(implDb, anotherSchemas)
      do! (db.Table<Person>()).Items |> Seq.toList |> assertEquals []
      let! _ = trap { it (db.Table<Song>()) }
      return ()
    }
