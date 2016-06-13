namespace TokiwaDb.Core

open System.IO
open System.Text

type StorageFile(_file: FileInfo) =
  inherit Storage()

  let createIfNotExists () =
    if not _file.Exists then
      use stream = _file.Create()
      in ()

  do createIfNotExists ()

  member this.ReadInt64(stream: FileStream) =
    let bytes = Array.zeroCreate 8
    let _ = stream.Read(bytes, 0, bytes.Length)
    in bytes |> Int64.ofByteArray

  member this.WriteInt64(stream: FileStream, n: int64) =
    let bytes = n |> Int64.toByteArray
    in stream.Write(bytes, 0, bytes.Length)

  member this.ReadData(p: pointer) =
    use stream    = _file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
    let _         = stream.Seek(p, SeekOrigin.Begin)
    let len       = this.ReadInt64(stream)
    assert (len <= (1L <<< 31))
    let data      = Array.zeroCreate (int len)  // TODO: Support "long" array.
    let _         = stream.Read(data, 0, int len)
    in data

  member this.WriteData(data: array<byte>): pointer =
    use stream    = _file.Open(FileMode.Append, FileAccess.Write)
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
