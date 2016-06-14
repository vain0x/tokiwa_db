namespace TokiwaDb.Core

open System
open System.IO

[<AutoOpen>]
module Extensions =
  type Storage with  
    member this.Derefer(recordPtr: RecordPointer): Record =
      recordPtr |> Array.map (fun valuePtr -> this.Derefer(valuePtr))

    member this.Store(record: Record): RecordPointer =
      record |> Array.map (fun value -> this.Store(value))

module ValuePointer =
  let ofUntyped (Field (_, type')) (p: int64) =
    match type' with
    | TInt      -> PInt p
    | TFloat    -> PFloat (BitConverter.Int64BitsToDouble p)
    | TString   -> PString p
    | TTime     -> PTime (DateTime.FromBinary(p))

  let toUntyped =
    function
    | PInt p    -> p
    | PFloat d  -> BitConverter.DoubleToInt64Bits(d)
    | PString p -> p
    | PTime t   -> t.ToBinary()

module Schema =
  let toFields (schema: Schema) =
    let keyFields =
      match schema.KeyFields with
      | Id -> [| Field ("id", TInt) |]
      | KeyFields keyFields -> keyFields
    in
      Array.append keyFields schema.NonkeyFields

module Mortal =
  let create t value =
    {
      Begin     = t
      End       = Int64.MaxValue
      Value     = value
    }

  let isAliveAt t (mortal: Mortal<_>) =
    mortal.Begin <= t && t < mortal.End

  let kill t (mortal: Mortal<_>) =
    if mortal |> isAliveAt t
    then { mortal with End = t }
    else mortal

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
