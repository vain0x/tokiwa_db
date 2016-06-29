namespace TokiwaDb.CodeFirst.Test

open System.Linq.Expressions
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module IndexSchemaTest =
  type Person = ModelTest.Person

  let fieldIndexesFromNamesTest =
    test {
      do! IndexSchema.fieldIndexesFromNames<Person> [| "Name"; "Age" |]
        |> assertEquals [| 1; 2 |]
    }

  let fieldIndexesFromLambdasTest =
    test {
      let lambdas =
        [|
          Expression.OfFun(fun (p: Person) -> p.Name :> obj)
          Expression.OfFun(fun (p: Person) -> p.Age :> obj)
        |]
        |> Array.map (fun lambda -> lambda :> Expression)
      do! IndexSchema.fieldIndexesFromLambdas<Person> lambdas
        |> assertEquals [| 1; 2 |]
    }

module UniqueIndexTest =
  type Person = ModelTest.Person

  let ofNamesTest =
    test {
      do! UniqueIndex.Of<Person>([| "Name"; "Age" |]).FieldIndexes
        |> assertEquals [| 1; 2 |]
    }

module TableSchemaTest =
  type Person = ModelTest.Person

  let ofModelTest =
    test {
      let expected =
        { TableSchema.empty "Person" with
            Fields = [| Field.string "Name"; Field.int "Age" |]
        }
      do! TableSchema.ofModel<Person> () |> assertEquals expected
    }

  let alterTest =
    test {
      let uniqueIndex = UniqueIndex.Of<Person>([| "Name"; "Age" |])
      let schema = TableSchema.ofModel<Person> ()
      do! schema |> TableSchema.alter [| uniqueIndex |]
        |> assertEquals { schema with Indexes = [| HashTableIndexSchema [| 1; 2 |] |] }
    }
