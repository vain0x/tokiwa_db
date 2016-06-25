namespace TokiwaDb.Core.Test

[<AutoOpen>]
module Persimmon =
  open Persimmon
  open Persimmon.Syntax.UseTestNameByReflection

  let testFun f (x, expected) =
    test {
      do! f x |> assertEquals expected
    }

  let assertSatisfies pred x =
    if pred x
    then pass ()
    else fail (sprintf "%A should satisfiy an expected property but didn't." x)
