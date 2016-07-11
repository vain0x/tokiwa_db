namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module DbConfigTest =
  let normalTest =
    test {
      let dbConfig = DbConfig<TestDbContext>()
      dbConfig.Add<Person>(UniqueIndex.Of<Person>([| "Name"; "Age" |]))
      dbConfig.OpenMemory("") |> ignore
      return ()
    }
