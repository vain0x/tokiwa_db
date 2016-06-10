namespace TokiwaDb.Core

[<AutoOpen>]
module Funcs =
  type Storage with  
    member this.Derefer(recordPtr: RecordPointer): Record =
      recordPtr |> Map.map (fun field valuePtr -> this.Derefer(valuePtr))

    member this.Store(record: Record): RecordPointer =
      record |> Map.map (fun field value -> this.Store(value))
