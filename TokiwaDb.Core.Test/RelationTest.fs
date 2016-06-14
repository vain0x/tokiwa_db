namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module RelationTest =
  /// NOTE: This is also used in TableTest.fs.
  module TestData =
    let storage = MemoryStorage()

    let makeRelation types records =
      Relation(types, records) :> IRelation

    let assertRelationEquals (expected: IRelation) (actual: IRelation) =
      actual.ToTuple() |> assertEquals (expected.ToTuple())

    let profiles =
      [
        ("Miku", 16, "Green Onions")
        ("Yukari", 18, "The Chainsaw")
        ("Kaito", 20, "Ices") // NOTE: Just a test data.
      ]
      |> List.map (fun (name, age, item) ->
        [|
          String name
          Int (int64 age)
          String item
        |] |> storage.Store
        )
      |> makeRelation
        [|
          Field ("name", TString)
          Field ("age", TInt)
          Field ("item", TString)
        |]

    let produces =
      [
        ("wowaka", "Miku")
        ("LIQ", "Miku")
        ("LIQ", "Yukari")
        ("hinayukki", "Kaito")
      ]
      |> List.map (fun (name, vocaloName) ->
        [|
          String name
          String vocaloName
        |] |> storage.Store
        )
      |> makeRelation
        [|
          Field ("p", TString)
          Field ("vocalo", TString)
        |]

  open TestData

  let projectionTest =
    test {
      let expected =
        [
          (16, "Miku")
          (18, "Yukari")
          (20, "Kaito")
        ]
        |> List.map (fun (age, name) ->
          [|Int (int64 age); String name|] |> storage.Store)
        |> makeRelation
          [|
            Field ("age", TInt)
            Field ("name", TString)
          |]
      let actual =
        profiles.Projection([|"age"; "name"|])
      do! actual |> assertRelationEquals expected
    }

  let renameTest =
    test {
      let expected =
        let fields =
          [|
            Field ("p_name", TString)
            Field ("vocaloid", TString)
          |]
        in Relation(fields, produces.RecordPointers)
      let actual = produces.Rename(Map.ofList [("p", "p_name"); ("vocalo", "vocaloid")])
      do! actual |> assertRelationEquals expected
    }

  let extendTest =
    test {
      let expected =
        [
          ("wowaka", "Miku", "wowaka feat. Miku")
          ("LIQ", "Miku", "LIQ feat. Miku")
          ("LIQ", "Yukari", "LIQ feat. Yukari")
          ("hinayukki", "Kaito", "hinayukki feat. Kaito")
        ]
        |> List.map (fun (p, vocalo, artist) ->
          [|String p; String vocalo; String artist|]
          |> storage.Store
          )
        |> makeRelation
          [|
            Field ("p", TString)
            Field ("vocalo", TString)
            Field ("artist", TString)
          |]
      let actual =
        let fields =
          Array.append produces.Fields [| Field ("artist", TString) |]
        let f (recordPointer: RecordPointer) =
          let record = storage.Derefer(recordPointer)
          match (record.[0], record.[1]) with
          | (String p, String vocalo) ->
            let artist    = sprintf "%s feat. %s" p vocalo
            let record'   = Array.append record [| String artist |]
            in storage.Store(record')
          | _ -> failwith ""
        in
          produces.Extend(fields, f)
      do! actual |> assertRelationEquals expected
    }

  let joinTest =
    test {
      let expected =
        [
          (16, "Green Onions", "Miku", "wowaka")
          (16, "Green Onions", "Miku", "LIQ")
          (18, "The Chainsaw", "Yukari", "LIQ")
          (20, "Ices", "Kaito", "hinayukki")
        ]
        |> List.map (fun (age, item, name, p) ->
          [| Int (int64 age); String item; String name; String p |]
          |> storage.Store
          )
        |> makeRelation
          [|
            Field ("age", TInt)
            Field ("item", TString)
            Field ("name", TString)
            Field ("p", TString)
          |]
      let actual =
        profiles.JoinOn([|"name"|], [|"vocalo"|], produces)
      do! actual |> assertRelationEquals expected
    }
