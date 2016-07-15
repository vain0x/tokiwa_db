namespace TokiwaDb.CodeFirst.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core
open TokiwaDb.Core.Test
open TokiwaDb.CodeFirst.Detail

module FSharpValueTest =
  module LazyTest =
    let ofClosureTest =
      test {
        let actual = FSharpValue.Lazy.ofClosure typeof<int> (fun () -> 1) :?> Lazy<int>
        do! actual.Value |> assertEquals 1
      }
