namespace TokiwaDb.Core.Test

open System.IO
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

  let setAllTest (xs: IResizeArray<string>) =
    test {
      let ()      = xs.Initialize(3L, "****")
      let ()      = xs.SetAll(fun xs ->
        xs.Set(0L, "0th.")
        xs.Set(1L, "1st.")
        )
      do! xs |> assertSeqEquals ["0th."; "1st."]
    }

  let toSeqTest (xs: IResizeArray<string>) =
    test {
      let ()      = xs.Set(0L, "0th.")
      let ()      = xs.Set(1L, "1st.")
      do! xs |> assertSeqEquals ["0th."; "1st."]
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
      StreamArray(new MemoryStreamSource(), FixedStringSerializer()) :> IResizeArray<_>
    in
      arrayTest xs
