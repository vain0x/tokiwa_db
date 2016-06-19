namespace TokiwaDb.Core.Test

open System
open System.IO
open System.Text
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StorageTest =
  let seaquentialStorageTest =
    test {
      let enc = UTF8Encoding.UTF8
      let mss = new MemoryStreamSource([||]) :> IStreamSource
      let ss = SequentialStorage(mss)
      let ps =
        seq {
          for s in [|"hello"; "world!"|] do
            yield ss.WriteData(enc.GetBytes(s))
        }
        |> Seq.toArray
      do! ss.ReadData(ps.[0]) |> enc.GetString |> assertEquals "hello"
      do! ss.ReadData(ps.[1]) |> enc.GetString |> assertEquals "world!"
    }

  let storageTest (storage: Storage) =
    let seeds             =
      [
        String "hello, world!"
        String "こんにちわ世界"
        String "hello, world!"
      ]
    /// Storage can store values.
    let values =
      [ for v in seeds do
          let p = storage.Store(v)
          yield (p, v)
      ]
    /// Storage can convert a pointer to its original value.
    let dereferTests =
      values |> List.map (fun (p, v) ->
        test {
          do! storage.Derefer p |> assertEquals v
        })
    /// If't forbidden to store a value in more than one places.
    let pointerTests =
      values |> Seq.groupBy (fun (p, v) -> v)
      |> Seq.map (fun (v, ps) ->
        test {
          do! ps |> Seq.equalAll |> assertPred
        })
    /// Concat tests.
    seq {
      yield! dereferTests
      yield! pointerTests
    }

  let streamSourceStorageTest =
    StreamSourceStorage(new MemoryStreamSource()) |> storageTest 
    
  let memoryStorageTest =
    MemoryStorage() |> storageTest
