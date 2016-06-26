namespace TokiwaDb.Core.Test

open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StreamSourceTest =
  let streamSourceTest (ss: StreamSource) =
    test {
      /// Can open and write.
      let stream = ss.OpenAppend()
      do stream.WriteByte(1uy)
      do stream.WriteByte(2uy)
      let () = stream.Close()
      /// Can get length.
      do! ss.Length |> assertEquals 2L
      /// Can re-open and read.
      let stream = ss.OpenRead()
      do! stream.ReadByte() |> assertEquals 1
      do! stream.ReadByte() |> assertEquals 2
      let () = stream.Close()
      // Clear test.
      do ss.Clear()
      do! ss.Length |> assertEquals 0L
      // WriteAll test.
      do ss.WriteString("****")
      do ss.WriteAll (fun ss -> ss.WriteString("hello"))
      do! ss.ReadString() |> assertEquals "hello"
      return ()
    }

  let memoryStreamSourceTest =
    let mss = new MemoryStreamSource([||])
    in mss |> streamSourceTest
