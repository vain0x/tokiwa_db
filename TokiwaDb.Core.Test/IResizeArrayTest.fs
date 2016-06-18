namespace TokiwaDb.Core.Test

open System.Text
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StreamArrayTest =
  let initializeTest (xs: IResizeArray<string>) =
    test {
      let ()      = xs.Initialize(2L, "****")
      do! xs.Length |> assertEquals 2L
      do! xs.Get(0L) |> assertEquals "****"
    }

  let setGetTest (xs: IResizeArray<string>) =
    test {
      let ()      = xs.Set(0L, "0th.")
      let ()      = xs.Set(1L, "1st.")
      let _       = trap { it (xs.Get(-1L)) } 
      let _       = trap { it (xs.Get(3L)) } 
      do! xs.Get(0L) |> assertEquals "0th."
      do! xs.Get(1L) |> assertEquals "1st."
      do! xs.Get(2L) |> assertEquals "test"
    }

  let arrayTest xs =
    test {
      do! initializeTest (xs ())
      do! setGetTest (xs ())
    }

  let memoryStreamArrayTest =
    let xs () =
      StreamArray(new MemoryStreamSource(), FixedStringSerializer()) :> IResizeArray<_>
    in
      arrayTest xs
