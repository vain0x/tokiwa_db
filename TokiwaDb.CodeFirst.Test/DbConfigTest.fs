namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module DbConfigTest =
  let dbConfig () =
    let dbConfig = DbConfig<TestDbContext>()
    dbConfig.Add<Person>(UniqueIndex.Of<Person>([| "Name"; "Age" |]))
    dbConfig

  let openTest =
    test {
      let dbConfig = dbConfig ()
      dbConfig.OpenMemory("") |> ignore
      return ()
    }

  let uniqueIndexTest =
    test {
      let dbConfig = dbConfig ()
      let db = dbConfig.OpenMemory("")
      db.Persons.Insert(Person(Name = "Miku", Age = 16L))
      let _ = trap { it (db.Persons.Insert(Person(Name = "Miku", Age = 18L))) }
      return ()
    }
