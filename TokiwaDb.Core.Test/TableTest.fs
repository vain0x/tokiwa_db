namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module TableTest =
  open TokiwaDb.Core.Test.RelationTest.TestData

  let rev = RevisionServer() :> IRevisionServer

  let persons =
    let schema =
      {
        KeyFields = Id
        NonkeyFields =
          [|
            Field ("name", TString)
            Field ("age", TInt)
          |]
      }
    in
      StreamTable("persons", schema, new MemoryStreamSource()) :> ITable

  let ``Insert to table with AI key`` =
    test {
      let record = [| String "Miku"; Int 16L |]
      do persons.Insert(rev, storage.Store(record))
      let actual =
        persons.Relation(rev.Current).RecordPointers
        |> Seq.map storage.Derefer
        |> Seq.toList
      do! actual |> assertEquals [Array.append [| Int 0L |] record]
    }
    
