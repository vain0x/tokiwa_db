namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StreamArrayTest =
  let initializeTest (xs: IResizeArray<float>) =
    test {
      let ()      = xs.Initialize(2L, Math.PI)
      do! xs.Length |> assertEquals 2L
      do! xs.Get(0L) |> assertEquals Math.PI
    }

  let setGetTest (xs: IResizeArray<float>) =
    test {
      let ()      = xs.Set(0L, 0.0)
      let ()      = xs.Set(1L, 1.0)
      let _       = trap { it (xs.Get(-1L)) } 
      let _       = trap { it (xs.Get(3L)) } 
      do! xs.Get(0L) |> assertEquals 0.0
      do! xs.Get(1L) |> assertEquals 1.0
      do! xs.Get(2L) |> assertEquals Math.PI
    }

  let setAllTest (xs: IResizeArray<float>) =
    test {
      let ()      = xs.Initialize(3L, Math.PI)
      let ()      = xs.SetAll(fun xs ->
        xs.Set(0L, 0.0)
        xs.Set(1L, 1.0)
        )
      do! xs |> assertSeqEquals [0.0; 1.0]
    }

  let toSeqTest (xs: IResizeArray<float>) =
    test {
      let ()      = xs.Set(0L, 0.0)
      let ()      = xs.Set(1L, 1.0)
      do! xs |> assertSeqEquals [0.0; 1.0]
    }

  let arrayTest xs =
    test {
      do! initializeTest (xs ())
      do! setGetTest (xs ())
      do! setAllTest (xs ())
      do! toSeqTest (xs ())
    }

  let memoryStreamArrayTest =
    let xs () =
      StreamArray(new MemoryStreamSource()) :> IResizeArray<_>
    in
      arrayTest xs
