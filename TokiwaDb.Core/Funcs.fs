namespace TokiwaDb.Core

open System.IO

[<AutoOpen>]
module Extensions =
  type Storage with  
    member this.Derefer(recordPtr: RecordPointer): Record =
      recordPtr |> Array.map (fun valuePtr -> this.Derefer(valuePtr))

    member this.Store(record: Record): RecordPointer =
      record |> Array.map (fun value -> this.Store(value))

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

type IStreamSource =
  abstract member OpenRead: unit -> Stream
  abstract member OpenAppend: unit -> Stream

type WriteOnceFileStreamSource(_file: FileInfo) =
  interface IStreamSource with
    override this.OpenRead() = _file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite) :> Stream
    override this.OpenAppend() = _file.Open(FileMode.Append, FileAccess.Write, FileShare.Read) :> Stream

type MemoryStreamSource(_buffer: array<byte>) =
  inherit MemoryStream()

  let mutable _buffer = _buffer

  new() = new MemoryStreamSource([||])

  member private this.Open(index) =
    let stream =
      { new MemoryStream() with
          override this.Close() =
            _buffer <- this.ToArray()
            base.Close()
      }
    let ()    = stream.Write(_buffer, 0, _buffer.Length)
    let _     = stream.Seek(index, SeekOrigin.Begin)
    in stream

  interface IStreamSource with
    override this.OpenRead() =
      this.Open(index = 0L) :> Stream

    override this.OpenAppend() =
      this.Open(index = _buffer.LongLength) :> Stream
