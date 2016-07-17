namespace TokiwaDb.CodeFirst.Test

open System.Linq.Expressions
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst
open TokiwaDb.CodeFirst.Detail

module IndexSchemaTest =
  let fieldIndexesFromNamesTest =
    test {
      do! IndexSchema.fieldIndexesFromNames typeof<Person> [| "Name"; "Age" |]
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
      do! IndexSchema.fieldIndexesFromLambdas typeof<Person> lambdas
        |> assertEquals [| 1; 2 |]
    }

module UniqueIndexTest =
  let ofNamesTest =
    test {
      do! UniqueIndex.Of<Person>([| "Name"; "Age" |]).FieldIndexes
        |> assertEquals [| 1; 2 |]
    }

module TableSchemaTest =
  let ofModelTest =
    test {
      let expected =
        { TableSchema.empty "Person" with
            Fields = [| Field.string "Name"; Field.int "Age" |]
        }
      do! TableSchema.ofModel typeof<Person> |> assertEquals expected
    }

  let alterTest =
    test {
      let uniqueIndex = UniqueIndex.Of<Person>([| "Name"; "Age" |])
      let schema = TableSchema.ofModel typeof<Person>
      do! schema |> TableSchema.alter [| uniqueIndex |]
        |> assertEquals { schema with Indexes = [| HashTableIndexSchema [| 1; 2 |] |] }
    }
