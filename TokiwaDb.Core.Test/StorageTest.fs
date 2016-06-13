namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StorageTest =
  let storageTest (storage: Storage) =
    let seeds             =
      [
        String "hello, world!"
        String "こんにちわ世界"
        String "hello, world!"
      ]
    let values =
      [ for v in seeds do
          let p = storage.Store(v)
          yield (p, v)
      ]
    let dereferTests =
      values |> List.map (fun (p, v) ->
        test {
          do! storage.Derefer p |> assertEquals v
        })
    let pointerTests =
      values |> Seq.groupBy (fun (p, v) -> v)
      |> Seq.map (fun (v, ps) ->
        test {
          do! ps |> Seq.equalAll |> assertPred
        })
    seq {
      yield! dereferTests
      yield! pointerTests
    }

  let streamSourceStorageTest =
    StreamSourceStorage(new MemoryStreamSource()) |> storageTest 
    
  let memoryStorageTest =
    MemoryStorage() |> storageTest
