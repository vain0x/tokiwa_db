namespace TokiwaDb.CodeFirst.Test

open System.Linq.Expressions
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module ModelTest =
  type Person(_name: string) =
    inherit Model()

    let mutable _name = _name

    new() =
      Person("")

    member this.Name
      with get () = _name
      and private set v = _name <- v

    member val Age = 0L with get, set

    member this.PropertiesWithoutSetterAreIgnored = null
    member private this.PrivatePropertiesAreIgnored = null
    member val FieldsAreIgnored = null

  let mappedPropertiesTest =
    test {
      do! Model.mappedProperties<Person> ()
        |> Array.map (fun pi -> pi.Name)
        |> assertEquals [| "Name"; "Age" |]
    }

  let toFieldsTest =
    test {
      do! Model.toFields<Person> ()
        |> assertEquals [| Field.string "Name"; Field.int "Age" |]
    }

  let toRecordTest =
    test {
      let person = Person("Miku", Age = 16L)
      do! person |> Model.toRecord
        |> assertEquals [| String "Miku"; Int 16L |]
    }

  let ofMortalRecordTest =
    test {
      let person =
        Mortal.create 0L [| Int -1L; String "Miku"; Int 16L |]
        |> Model.ofMortalRecord<Person>
      do! person.Id |> assertEquals -1L
      do! person.Name |> assertEquals "Miku"
      do! person.Age |> assertEquals 16L
    }
