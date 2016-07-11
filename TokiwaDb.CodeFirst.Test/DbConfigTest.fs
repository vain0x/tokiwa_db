namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module DbConfigTest =
  let normalTest =
    test {
      let dbConfig = DbConfig()
      dbConfig.AddTable<Person>(UniqueIndex.Of<Person>([| "Name"; "Age" |]))
      dbConfig.OpenMemory("") |> ignore
      return ()
    }

  let addTableFailureTest =
    test {
      let dbConfig = DbConfig()
      dbConfig.AddTable<Person>()
      let! e = trap { it (dbConfig.AddTable<Person>()) }
      return ()
    }
