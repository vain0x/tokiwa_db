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

  let ``Insert and TryFind Test`` =
    test {
      let hti = hashTableIndex ()
      do! hti.TryFind([| PInt 3L; PInt 2L |]) |> assertEquals (Some 1L)
      do! hti.TryFind([| PInt 4L; PInt 0L |]) |> assertEquals None
    }

  let ``Insert and Delete Test`` =
    test {
      let hti       = hashTableIndex ()
      let record    = [| PInt 3L; PInt 2L |]
      do! hti.Remove(record) |> assertEquals true
      do! hti.TryFind(record) |> assertEquals None
    }
