namespace TokiwaDb.CodeFirst.Test

open System
open System.Linq.Expressions
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.Core.Test
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module ValuePointerTest =
  let fakeCoerce =
    function
    | PInt x        -> Int x
    | PString _     -> String "Miku"
    | _ -> NotImplementedException() |> raise

  let toObjNormalTest =
    let body (vp, typ, expected) =
      test {
        do! vp |> ValuePointer.toObj typ fakeCoerce |> assertEquals expected
      }
    parameterize {
      case (PInt 0L     , typeof<int64>         , 0L :> obj)
      case (PString 0L  , typeof<string>        , "Miku" :> obj)
      run body
    }

  let testToObjLazy<'x> expected vp =
    test {
      let result      = vp |> ValuePointer.toObj typeof<Lazy<'x>> fakeCoerce
      do! ((result :?> Lazy<'x>).Value :> obj) |> assertEquals expected
    }

  let toObjLazyTest =
    test {
      do! PInt 0L |> testToObjLazy 0L
      do! PString 0L |> testToObjLazy "Miku"
    }

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
      do! Model.mappedProperties typeof<Person>
        |> Array.map (fun pi -> pi.Name)
        |> assertEquals [| "Name"; "Age" |]
    }

  let toFieldsTest =
    test {
      do! Model.toFields typeof<Person>
        |> assertEquals [| Field.string "Name"; Field.int "Age" |]
    }

  let toRecordTest =
    test {
      let person = Person("Miku", Age = 16L)
      do! person |> Model.toRecord typeof<Person>
        |> assertEquals [| String "Miku"; Int 16L |]
    }

  let ofMortalRecordPointerTest =
    test {
      let coerce =
        function
        | PInt x        -> Int x
        | PString 0L    -> String "Miku"
        | _ -> NotImplementedException() |> raise
      let person =
        Mortal.create 0L [| PInt -1L; PString 0L; PInt 16L |]
        |> Model.ofMortalRecordPointer typeof<Person> coerce
        :?> Person
      do! person.Id |> assertEquals -1L
      do! person.Name |> assertEquals "Miku"
      do! person.Age |> assertEquals 16L
    }

[<AutoOpen>]
module ModelsForTest =
  type Person() =
    inherit Model()

    member val Name = "" with get, set
    member val Age = 0L with get, set

  type Song() =
    inherit Model()

    member val Name = "" with get, set
    member val Vocal = lazy "" with get, set
