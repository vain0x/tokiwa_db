namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StorageTest =
  let storageFileTest =
    let file = FileInfo(@"__test.tokiwa_storage")
    if file.Exists then file.Delete()
    test {
      let storage           = StorageFile(file)
      do! file.Exists |> assertPred
      let pEnglishHello     = storage.WriteString("hello world!")
      let pJapaneseHello    = storage.WriteString("ハローワールド")
      do! pEnglishHello  |> assertEquals 0L
      do! pJapaneseHello |> assertEquals 20L
      do! storage.ReadString(pEnglishHello)  |> assertEquals "hello world!"
      do! storage.ReadString(pJapaneseHello) |> assertEquals "ハローワールド"
    }
    
  let storageTest (storage: Storage) =
    let seeds             =
      [
        String "hello, world!"
        String "こんにちわ世界"
      ]
    let values =
      [ for v in seeds do
          let p = storage.Store(v)
          yield (p, v)
      ]
    in
      values |> List.map (fun (p, v) -> test {
        do! storage.Derefer p |> assertEquals v
        })

  let memoryStorageTest =
    let storage = MemoryStorage()
    in storageTest storage
