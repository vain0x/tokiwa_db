namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module OrmDatabaseTest =
  let schemas =
    [
      (typeof<Person>, TableSchema.ofModel typeof<Person>)
      (typeof<Song>, TableSchema.ofModel typeof<Song>)
    ]

  let testDb () =
    let db      = new OrmDatabase(new MemoryDatabase("test_db"), schemas)
    in db.CreateContext<TestDbContext>()

  let createTest =
    test {
      let db = testDb ()
      do! db.Database.Name |> assertEquals "test_db"
    }

  let tableTest =
    test {
      let db = testDb ()
      do! db.Persons.Name |> assertEquals "Person"
      return ()
    }

  type AnotherDbContext =
    {
      Database          : Database
      Persons           : Table<Person>
    }

  let anotherSchemas = [schemas |> List.head]

  let reopenTest =
    test {
      let implDb = new MemoryDatabase("test_db")
      do! test {
        use db = new OrmDatabase(implDb, schemas)
        let context = db.CreateContext<TestDbContext>()
        context.Persons.Insert(Person(Name = "Miku", Age = 16L))
        return ()
      }
      // We can reopen the database with the same models.
      do! test {
        use db = new OrmDatabase(implDb, schemas)
        let context = db.CreateContext<TestDbContext>()
        do! context.Persons.CountAllRecords |> assertEquals 1L
      }
      // Opening with different models, all tables are dropped.
      do! test {
        use db = new OrmDatabase(implDb, anotherSchemas)
        let context = db.CreateContext<AnotherDbContext>()
        do! context.Persons.CountAllRecords |> assertEquals 0L
        return ()
      }
    }
