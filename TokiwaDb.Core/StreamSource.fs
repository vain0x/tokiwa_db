namespace TokiwaDb.Core

open System.IO
open System.Text

type [<AbstractClass>] StreamSource() =
  abstract member OpenReadWrite: unit -> Stream
  abstract member OpenRead: unit -> Stream
  abstract member OpenAppend: unit -> Stream
  abstract member Clear: unit -> unit
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

  override this.Length =
    _file |> FileInfo.length

type MemoryStreamSource(_buffer: array<byte>) =
  inherit StreamSource()

  let mutable _buffer = _buffer

  let _open index =
    let stream =
      { new MemoryStream() with
          override this.Close() =
            _buffer <- this.ToArray()
            base.Close()
      }
    let ()    = stream.Write(_buffer, 0, _buffer.Length)
    let _     = stream.Seek(index, SeekOrigin.Begin)
    in stream

  new() = new MemoryStreamSource([||])

  override this.OpenReadWrite() =
    _open 0L :> Stream

  override this.OpenRead() =
    this.OpenReadWrite()

  override this.OpenAppend() =
    _open _buffer.LongLength :> Stream

  override this.Clear() =
    _buffer <- [||]

  override this.Length =
    _buffer.LongLength
