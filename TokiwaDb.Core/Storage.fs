namespace TokiwaDb.Core

open System.Collections.Generic
open System.IO
open System.Text

type StreamSourceStorage(_src: IStreamSource) =
  inherit Storage()

  member this.ReadInt64(stream: Stream) =
    let bytes = Array.zeroCreate 8
    let _ = stream.Read(bytes, 0, bytes.Length)
    in bytes |> Int64.ofByteArray

  member this.WriteInt64(stream: Stream, n: int64) =
    let bytes = n |> Int64.toByteArray
    in stream.Write(bytes, 0, bytes.Length)

  member this.ReadData(p: pointer) =
    use stream    = _src.OpenRead()
    let _         = stream.Seek(p, SeekOrigin.Begin)
    let len       = this.ReadInt64(stream)
    assert (len <= (1L <<< 31))
    let data      = Array.zeroCreate (int len)  // TODO: Support "long" array.
    let _         = stream.Read(data, 0, int len)
    in data

  member this.WriteData(data: array<byte>): pointer =
    use stream    = _src.OpenAppend()
    let p         = stream.Position
    let length    = 8L + data.LongLength
    let ()        = this.WriteInt64(stream, data.LongLength)
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

  let _lookup p = _dict.Item(p)

  let _add value =
    let k     = _dict.Count |> int64
    let ()    = _dict.Add(k, value)
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
