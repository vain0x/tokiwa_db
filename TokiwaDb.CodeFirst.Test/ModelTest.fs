namespace TokiwaDb.CodeFirst.Test

open System.Linq.Expressions
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module ModelTest =
  type Person(_name: string) =
    inherit Model<Person>()

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
      let person = Person("Miku", Age = 18L)
      do! person |> Model.toRecord
        |> assertEquals [| String "Miku"; Int 18L |]
    }

  let ofRecordTest =
    test {
      let person = [| Int -1L; String "Miku"; Int 18L |] |> Model.ofRecord<Person>
      do! person.Id |> assertEquals -1L
      do! person.Name |> assertEquals "Miku"
      do! person.Age |> assertEquals 18L
    }
