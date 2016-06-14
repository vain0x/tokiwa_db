namespace TokiwaDb.Core.Test

open System
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module ValuePointerTest =
  let ``toUntyped and ofUntyped Test`` =
    let body (valuePointer, type') =
      test {
        let actual = valuePointer |> ValuePointer.toUntyped |> ValuePointer.ofUntyped type'
        do! actual |> assertEquals valuePointer
      }
    parameterize {
      case (PInt -12345678L, TInt)
      case (PTime DateTime.Now, TTime)
      case (PString 8L, TString)
      case (PFloat 3.1415926535897932384626433832795, TFloat)
      case (PFloat Double.PositiveInfinity, TFloat)
      case (PFloat Double.Epsilon, TFloat)
      run body
    }

module RevisionServerTest =
  let revisionServerTest =
    test {
      let rs = RevisionServer() :> IRevisionServer
      do! rs.Current |> assertEquals 0L
      do! rs.Next() |> assertEquals 1L
      do! rs.Next() |> assertEquals 2L
      do! rs.Current |> assertEquals 2L
    }
