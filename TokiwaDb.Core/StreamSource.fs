namespace TokiwaDb.Core

open System.IO
open System.Text

type [<AbstractClass>] StreamSource() =
  abstract member OpenReadWrite: unit -> Stream
  abstract member OpenRead: unit -> Stream
  abstract member OpenAppend: unit -> Stream
  abstract member Clear: unit -> unit
  abstract member WriteAll: (StreamSource -> unit) -> unit
  abstract member Length: int64

  member this.ReadString() =
    use stream        = this.OpenRead()
    let data          = Array.zeroCreate (stream.Length |> int)
    let _             = stream.Read(data, 0, data.Length)
    in UTF8Encoding.UTF8.GetString(data)

  member this.WriteString(content: string) =
    let ()            = this.Clear()
    let data          = UTF8Encoding.UTF8.GetBytes(content)
    use stream        = this.OpenAppend()
    let ()            = stream.Write(data, 0, data.Length)
    in ()

/// Stream source based on a file.
/// Note: OpenRead doesn't lock the file for reading or writing.
type FileStreamSource(_file: FileInfo) =
  inherit StreamSource()

  override this.OpenReadWrite() =
    _file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite) :> Stream

  override this.OpenRead() =
    _file.Open(FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite) :> Stream

  override this.OpenAppend() =
    _file.Open(FileMode.Append, FileAccess.Write, FileShare.Read) :> Stream

  override this.Clear() =
    if _file |> FileInfo.exists then
      _file.Delete()

  override this.WriteAll(writeTo) =
    let temporaryFile   = FileInfo(Path.GetTempFileName())
    FileStreamSource(temporaryFile) |> writeTo
    this.Clear()
    temporaryFile.MoveTo(_file.FullName)

  override this.Length =
    _file |> FileInfo.length

type ReopenableMemoryStream(_initialValue: array<byte>, _initialPosition: int64) =
  inherit Stream()

  let _stream =
    let stream = new MemoryStream()
    let () =
      stream.Write(_initialValue, 0, _initialValue.Length)
      stream.Position <- _initialPosition
    in stream

  member this.ToArray() =
    _stream.ToArray()

  override this.CanRead       = true
  override this.CanSeek       = true
  override this.CanTimeout    = false
  override this.CanWrite      = true
  override this.Length        = _stream.Length
  override this.ReadTimeout   = _stream.ReadTimeout
  override this.WriteTimeout  = _stream.WriteTimeout

  override this.Position
    with get () = _stream.Position
    and  set v  = _stream.Position <- v

  override this.Flush()       = ()

  override this.Seek(offset, origin) =
    _stream.Seek(offset, origin)

  override this.SetLength(newLength) =
    _stream.SetLength(newLength)

  override this.Read(buffer, offset, count) =
    _stream.Read(buffer, offset, count)

  override this.ReadByte() =
    _stream.ReadByte()

  override this.Write(data, offset, count) =
    _stream.Write(data, offset, count)

  override this.WriteByte(value) =
    _stream.WriteByte(value)

  new (value: array<byte>) =
    new ReopenableMemoryStream(value, 0L)

  new () =
    new ReopenableMemoryStream([||])

type MemoryStreamSource(_buffer: array<byte>) =
  inherit StreamSource()

  let mutable _stream =
    new ReopenableMemoryStream(_buffer)

  let _open position =
    _stream.Position <- position
    _stream :> Stream

  new() = new MemoryStreamSource([||])

  member private this.UnderlyingStream() =
    _stream

  override this.OpenReadWrite() =
    _open 0L

  override this.OpenRead() =
    _open 0L

  override this.OpenAppend() =
    _open _stream.Length

  override this.Clear() =
    _stream.SetLength(0L)

  override this.WriteAll(writeTo) =
    let temporarySource   = MemoryStreamSource ()
    let ()                = temporarySource |> writeTo
    let ()                = _stream <- temporarySource.UnderlyingStream()
    in ()

  override this.Length =
    _stream.Length
