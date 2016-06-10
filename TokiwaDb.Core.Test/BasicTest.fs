namespace TokiwaDb.Core.Test

open System
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.Core.Types

module BasicTest =
  /// Sample DB which contains no data.
  let emptyDb = Storage(@"empty_db").Create()

  let createTest =
    test {
      do! emptyDb.Tables |> Seq.length |> assertEquals 0
    }

  /// Sample DB which contains sample data.
  let theDb =
    let storage = Storage(@"test_db")
    if storage.Exists then storage.Delete()
    let db = storage.Create()
    let fields =
      [
        Field.Id("id")
        Field.String("name")
        Field.Int("age")
        Field.Time("reg_time")
      ]
    let db = db.CreateTable("persons", fields)
    let person (name, age) =
      [
        ("name", Value.String name)
        ("age", Value.Int (int64 age))
        ("reg_time", Value.Time DateTime.Now)
      ]
    let persons =
      [
        ("Miku", 16)
        ("Rin", 14)
        ("Luka", 20)
      ]
      |> List.map person
    let db = db.InsertRange(persons)
    in db

  let tableDefinitionTest =
    test {
      do! theDb.Tables |> Seq.length |> assertEquals 1
      let table = theDb.Tables |> Seq.head
      let fields = table.Fields
      do! fields |> Seq.length |> assertEquals 4
    }

  let insertTest =
    test {
      let table = theDb.TryTableByName("persons") |> Option.get
      do! table.Count |> assertEquals 3
      let names =
        table
        |> Seq.map (fun record -> record.TryFieldByName("name") |> Option.get)
        |> Seq.toList
      do! names |> assertEquals ["Miku"; "Rin"; "Luka"]
    }
