namespace TokiwaDb.Core

open System.Collections.Generic
open System.IO
open System.Text

[<AbstractClass>]
type Storage() =
  abstract member Derefer: ValuePointer -> Value
  abstract member Store: Value -> ValuePointer

  member this.Derefer(recordPtr: RecordPointer): Record =
    recordPtr |> Array.map (fun valuePtr -> this.Derefer(valuePtr))

  member this.Store(record: Record): RecordPointer =
    record |> Array.map (fun value -> this.Store(value))

  interface IStorage with
    override this.Derefer(valuePointer) = this.Derefer(valuePointer)
    override this.Store(value) = this.Store(value)

type StreamSourceStorage(_src: IStreamSource) =
  inherit Storage()

  /// Reads the data written at the current position.
  /// Advances the position by the number of bytes read.
  member this.ReadData(stream: Stream) =
    let len       = stream |> Stream.readInt64
    // TODO: Support "long" array.
    assert (len <= (1L <<< 31))
    let len       = int len
    let data      = Array.zeroCreate len
    let _         = stream.Read(data, 0, len)
    in data

  member this.ReadData(p) =
    use stream    = _src.OpenRead()
    let _         = stream.Seek(p, SeekOrigin.Begin)
    in this.ReadData(stream)

  member this.ToSeq() =
    let stream      = _src.OpenRead()
    seq {
      while stream.Position < stream.Length do
        yield (stream.Position, this.ReadData(stream))
    }

  member this.TryFindData(data: array<byte>) =
    this.ToSeq()
    |> Seq.tryFind (fun (p, data') -> data = data')
    |> Option.map fst

  member this.WriteData(data: array<byte>): pointer =
    match this.TryFindData(data) with
    | Some p -> p
    | None ->
      use stream    = _src.OpenAppend()
      let p         = stream.Position
      let length    = 8L + data.LongLength
      let ()        = stream |> Stream.writeInt64 data.LongLength
      let ()        = stream.Write(data, 0, data.Length)
      in p

  member this.ReadString(p: pointer) =
    UTF8Encoding.UTF8.GetString(this.ReadData(p))

  member this.WriteString(s: string) =
    this.WriteData(UTF8Encoding.UTF8.GetBytes(s))

  override this.Derefer(valuePtr): Value =
    match valuePtr with
    | PInt x       -> Int x
    | PFloat x     -> Float x
    | PTime x      -> Time x
    | PString p    -> this.ReadString(p) |> String

  override this.Store(value) =
    match value with
    | Int x       -> PInt x
    | Float x     -> PFloat x
    | Time x      -> PTime x
    | String s    -> this.WriteString(s) |> PString

type FileStorage(_file: FileInfo) =
  inherit StreamSourceStorage(WriteOnceFileStreamSource(_file))

type MemoryStorage() =
  inherit Storage()

  let _dict = Dictionary<int64, Value>()

  let _rev = Dictionary<Value, int64>()

  let _lookup p = _dict.Item(p)

  let _add value =
    match _rev.TryGetValue(value) with
    | (true, p) -> p
    | (false, _) ->
      let k     = _dict.Count |> int64
      let ()    = _dict.Add(k, value)
      let ()    = _rev.Add(value, k)
      in k

  override this.Derefer(valuePtr): Value =
    match valuePtr with
    | PInt x       -> Int x
    | PFloat x     -> Float x
    | PTime x      -> Time x
    | PString p
      -> _lookup p

  override this.Store(value) =
    match value with
    | Int x       -> PInt x
    | Float x     -> PFloat x
    | Time x      -> PTime x
    | String _
      -> _add value |> PString
