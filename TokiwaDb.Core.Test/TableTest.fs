namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module TableTest =
  let testDb = MemoryDatabase("testDb")
  let storage = testDb.Storage
  let rev = testDb.RevisionServer

  let ``Insert to table with AI key`` =
    let schema =
      {
        KeyFields = Id
        NonkeyFields =
          [|
            Field ("name", TString)
            Field ("age", TInt)
          |]
      }
    let persons =
      testDb.CreateTable("persons", schema)
    let testData =
      [
        [| String "Miku"; Int 16L |]
        [| String "Yukari"; Int 18L |]
        [| String "Kaito"; Int 20L |]
      ]
    for record in testData do
      persons.Insert(storage.Store(record))
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

  let ``Insert to table with non-AI key`` =
    let schema =
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
    let songs = testDb.CreateTable("songs", schema)
    let testData =
      [
        [| String "Ura Omote Lovers"; String "wowaka"; String "Miku" |]
        [| String "Frontier"; String "LIQ"; String "Yukari" |]
      ]
    for record in testData do
      songs.Insert(storage.Store(record))
    test {
      let actual =
        songs.Relation(rev.Current).RecordPointers
        |> Seq.map storage.Derefer
        |> Seq.toList
      do! actual |> assertEquals testData
    }

  module TestData =
    let testDb = testDb

    let persons =
      testDb.Tables(testDb.RevisionServer.Current)
      |> Seq.find (fun table -> table.Name = "persons")

  open TestData

  let deleteTest =
    test {
      let previousRevisionId = testDb.RevisionServer.Current
      let pred x =
        match x with
        | [| _; _; PInt age |] when age >= 20L -> true
        | _ -> false
      do persons.Delete(pred)
      let actual = persons.Relation(testDb.RevisionServer.Current).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 2
      /// And the previous version is still available.
      let actual = persons.Relation(previousRevisionId).RecordPointers |> Seq.toList
      do! actual |> List.length |> assertEquals 3
    }
