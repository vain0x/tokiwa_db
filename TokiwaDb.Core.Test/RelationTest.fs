namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module RelationTest =
  let storage = MemoryStorage()

  let makeRelation name types records =
    MemoryRelation(name, types, records)

  let profiles =
    [
      ("Miku", 16, "Green Onions")
      ("Yukari", 18, "The Chainsaw")
      ("Kaito", 20, "Ices") // Just a test data.
    ]
    |> List.map (fun (name, age, item) ->
      [
        ("name", String name)
        ("age", Int (int64 age))
        ("item", String item)
      ] |> Map.ofList)
    |> makeRelation "profiles"
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
      [
        ("p", String name)
        ("vocalo", String vocaloName)
      ])

  let projectionTest =
    test {
      let expected =
        [
          ("Miku", 16)
          ("Yukari", 18)
          ("Kaito", 20)
        ]
        |> List.map (fun (name, age) -> [String name; Int (int64 age)])
      do! profiles.Projection(set ["name"; "age"]) |> assertEquals expected
    }
