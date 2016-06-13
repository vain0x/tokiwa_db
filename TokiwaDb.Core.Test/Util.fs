namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection

module Persimmon =
  let testFun f (x, expected) =
    test {
      do! (f x) |> assertEquals expected
    }
