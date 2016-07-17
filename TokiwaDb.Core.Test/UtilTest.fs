namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module OptionTest =
  let sequenceTest =
    parameterize {
      case (seq [], Some [])
      case (seq [Some 0; Some 1; Some 2], Some [0; 1; 2])
      case (seq [Some 0; None; Some 2], None)
      case (seq [None; Some 1; Some 2], None)
      case (seq [None; None; None], None)
      run (Persimmon.testFun Option.sequence)
    }

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
