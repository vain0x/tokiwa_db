namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module SeqTest =
  let equalAllTest =
    parameterize {
      case (Seq.empty, true)
      case (seq [0], true)
      case (seq [0; 0; 0], true)
      case (seq [0; 0; 1], false)
      case (seq [1; 2; 3], false)
      run (Persimmon.testFun Seq.equalAll)
    }
