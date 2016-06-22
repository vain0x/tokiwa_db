namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module TableTest =
  let testDb = MemoryDatabase("testDb")
  let storage = testDb.Storage
  let rev = testDb.RevisionServer
  
  let testData =
    [
      [| String "Miku"; Int 16L |]
      [| String "Yukari"; Int 18L |]
      [| String "Kaito"; Int 20L |]
    ]

  let insertTest =
    let schema =
      {
        Name = "persons"
        Fields =
          [|
            Field.string "name"
            Field.int "age"
          |]
      }
    let persons =
      testDb.CreateTable(schema)
    for record in testData do
      persons.Insert(record)
    test {
      let expected =
        testData
        |> List.mapi (fun i record -> Array.append [| Int (int64 i) |] record)
      let actual =
        persons.Relation(rev.Current).RecordPointers
        |> Seq.map storage.Derefer
        |> Seq.toList
      do! actual |> assertEquals expected
    }
    
  module TestData =
    let testDb = testDb

    let persons =
      testDb.Tables(rev.Current)
      |> Seq.find (fun table -> table.Name = "persons")

  open TestData

  let recordByIdTest =
    test {
      let actual = persons.RecordById(0L)
      do! actual |> Option.isSome |> assertPred
    }

  let toSeqTest =
    let body (expected, (recordId, recordPointer: Mortal<RecordPointer>)) =
      test {
        do! storage.Derefer(recordPointer.Value).[1..] |> assertEquals expected
      }
    in
      parameterize {
        source (persons.ToSeq() |> Seq.toList |> List.zip testData)
        run body
      }

  let removeTest =
    test {
      let previousRevisionId = rev.Current
      // Remove Yukari.
      let actual = persons.Remove(1L) |> Option.map (fun rp -> rp.Value.[2])
      do! actual |> assertEquals (PInt 18L |> Some)
      let actual = persons.Relation(testDb.RevisionServer.Current).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 2
      /// And the previous version is still available.
      let actual = persons.Relation(previousRevisionId).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 3
    }

  let dropTest =
    test {
      do! testDb.DropTable("persons") |> assertEquals true
      do! testDb.Tables(rev.Current) |> Seq.exists (fun table -> table.Name = "persons") |> assertEquals false
      do! testDb.DropTable("INVALID NAME") |> assertEquals false
    }

  let ``Insert/Remove to table with an index`` =
    test {
      let schema =
        {
          Name = "persons2"
          Fields =
            [| Field.string "name"; Field.int "age" |]
        }
      // Create a table with index in "name" column.
      // NOTE: The first column (with index 0) is "id".
      let persons2 =
        testDb.CreateTable(schema, [| [| 1 |] |])
      let () =
        persons2.Insert([| String "Miku"; Int 16L |])
        persons2.Insert([| String "Yukari"; Int 18L |])
      do! persons2.Indexes.Length |> assertEquals 1
      let index       = persons2.Indexes.[0]
      do! index.TryFind(storage.Store([| String "Miku" |])) |> assertEquals (Some 0L)
      // Then remove Miku.
      let () =
        persons2.Remove(0L) |> ignore
      do! index.TryFind(storage.Store([| String "Miku"  |])) |> assertEquals None
    }
