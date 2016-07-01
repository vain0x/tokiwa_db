namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module HashTableIndexTest =
  let fieldIndexes = [|1; 0|]

  let empty () =
    StreamHashTableIndex(fieldIndexes, new MemoryStreamSource())

  let hashTableIndex () =
    let hti       = empty ()
    let ()        = hti.Insert([| PInt 1L; PInt 0L |], 0L)
    let ()        = hti.Insert([| PInt 3L; PInt 2L |], 1L)
    in hti

  let projectionTest =
    test {
      let hti = empty ()
      do! hti.Projection([| PInt 0L; PInt 1L; PInt 2L |]) |> assertEquals ([| PInt 1L; PInt 0L |])
    }

  let ``Insert and FindAll Test`` =
    test {
      let hti = hashTableIndex ()
      do! hti.FindAll([| PInt 3L; PInt 2L |]) |> assertSeqEquals [1L]
      do! hti.FindAll([| PInt 4L; PInt 0L |]) |> assertSeqEquals []
    }
