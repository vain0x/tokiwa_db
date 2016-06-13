namespace TokiwaDb.Core.Test

open System
open System.IO
open Persimmon
open Persimmon.Syntax.UseTestNameByReflection
open TokiwaDb.Core

module StorageFileTest =
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
