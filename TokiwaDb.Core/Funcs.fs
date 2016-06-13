namespace TokiwaDb.Core

[<AutoOpen>]
module Extensions =
  type Storage with  
    member this.Derefer(recordPtr: RecordPointer): Record =
      recordPtr |> Map.map (fun field valuePtr -> this.Derefer(valuePtr))

    member this.Store(record: Record): RecordPointer =
      record |> Map.map (fun field value -> this.Store(value))

module Int64 =
  let toByteArray n =
    Seq.unfold (fun (n, i) ->
      if i = 8
      then None
      else Some (byte (n &&& 0xFFL), (n >>> 8, i + 1))
      ) (n, 0)
    |> Seq.toArray
    |> Array.rev

  let ofByteArray (bs: array<byte>) =
    assert (bs.Length = 8)
    bs |> Array.fold (fun n b -> (n <<< 8) ||| (int64 b)) 0L
