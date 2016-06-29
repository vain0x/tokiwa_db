namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module DbConfigTest =
  let testDb =
    let dbConfig = DbConfig()
    dbConfig.AddTable<Person>(UniqueIndex.Of<Person>([| "Name"; "Age" |]))
    dbConfig.OpenMemory("")
