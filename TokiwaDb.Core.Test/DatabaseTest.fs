namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module DatabaseTest =
  let repo    = DirectoryInfo(@"__unit_test_db")
  let mutable savedRevision = 0L
  let insertedRow = [| String "Miku"; Int 16L|]

  if repo.Exists then
    repo.Delete((* recusive =*) true)

  let createTest =
    test {
      use db      = new DirectoryDatabase(repo)
      let rev     = db.Transaction.RevisionServer

      let persons =
        let schema =
          { TableSchema.empty "persons" with
              Fields = [| Field.string "name"; Field.int "age" |]
          }
        in db.CreateTable(schema)

      let actual = db.Tables(rev.Current) |> Seq.map (fun table -> table.Name) |> Seq.toList
      do! actual |> assertEquals [persons.Name]

      let _ = persons.Insert([| insertedRow |])

      savedRevision <- rev.Current
      return ()
    }

  let reopenTest =
    test {
      use db      = new DirectoryDatabase(repo)
      /// Revision number should be saved.
      do! db.CurrentRevisionId |> assertEquals savedRevision
      /// Tables should be loaded.
      let tables  = db.Tables(savedRevision)
      let actual  = tables |> Seq.map (fun table -> table.Name) |> Seq.toList
      do! actual |> assertEquals ["persons"]
      /// Inserted rows should be saved.
      let persons = tables |> Seq.find (fun table -> table.Name = "persons")
      let actual  = persons.Relation(savedRevision).RecordPointers |> Seq.head |> db.Storage.Derefer
      do! actual |> assertEquals (Array.append [| Int 0L |] insertedRow)
    }
