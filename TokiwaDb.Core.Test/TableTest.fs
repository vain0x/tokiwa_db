namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module TableTest =
  open TokiwaDb.Core.Test.RelationTest.TestData

  let rev () =
    RevisionServer() :> IRevisionServer

  let ``Insert to table with AI key`` =
    test {
      let schema =
        {
          KeyFields = Id
          NonkeyFields =
            [|
              Field ("name", TString)
              Field ("age", TInt)
            |]
        }
      let persons     = StreamTable("persons", schema, new MemoryStreamSource()) :> ITable
      let rev         = rev ()
      let record1     = [| String "Miku"; Int 16L |]
      let record2     = [| String "Yukari"; Int 18L |]
      let expected    =
        [
          Array.append [| Int 0L |] record1
          Array.append [| Int 1L |] record2
        ]
      do persons.Insert(rev, storage.Store(record1))
      do persons.Insert(rev, storage.Store(record2))
      let actual =
        persons.Relation(rev.Current).RecordPointers
        |> Seq.map storage.Derefer
        |> Seq.toList
      do! actual |> assertEquals expected
    }

  let ``Insert to table with non-AI key`` =
    test {
      let schema    =
        {
          KeyFields =
            KeyFields
              [|
                Field ("title", TString)
                Field ("composer", TString)
              |]
          NonkeyFields =
            [| Field ("singer", TString) |]
        }
      let songs     = StreamTable("songs", schema, new MemoryStreamSource()) :> ITable
      let rev       = rev ()
      let record1   = [| String "Ura Omote Lovers"; String "wowaka"; String "Miku" |]
      let record2   = [| String "Frontier"; String "LIQ"; String "Yukari" |]
      let expected  = [record1; record2]
      do songs.Insert(rev, storage.Store(record1))
      do songs.Insert(rev, storage.Store(record2))
      let actual =
        songs.Relation(rev.Current).RecordPointers
        |> Seq.map storage.Derefer
        |> Seq.toList
      do! actual |> assertEquals expected
    }

  module TestData =
    let rev = rev ()

    let persons () =
      let schema =
        {
          KeyFields = Id
          NonkeyFields =
            [|
              Field ("name", TString)
              Field ("age", TInt)
              Field ("item", TString)
            |]
        }
      let table = StreamTable("persons", schema, new MemoryStreamSource()) :> ITable
      for recordPointer in profiles.RecordPointers do
        table.Insert(rev, recordPointer)
      table

  open TestData

  let deleteTest =
    test {
      let persons = persons ()
      let previousRevisionId = rev.Current
      let pred =
        function
        | [| _; _; PInt age; _ |] when age < 20L -> true
        | _ -> false
      do persons.Delete(rev, pred)
      let actual = persons.Relation(rev.Current).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 1
      /// And the previous version is still available.
      let actual = persons.Relation(previousRevisionId).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 3
    }
