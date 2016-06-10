namespace TokiwaDb.Core

open System
open System.IO

type StorageFile(file: FileInfo) =
  interface IStorage with
    