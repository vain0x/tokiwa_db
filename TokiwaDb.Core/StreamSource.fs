namespace TokiwaDb.Core

open System.IO

type IStreamSource =
  abstract member OpenRead: unit -> Stream
  abstract member OpenAppend: unit -> Stream
  abstract member Length: int64

type WriteOnceFileStreamSource(_file: FileInfo) =
  interface IStreamSource with
    override this.OpenRead() = _file.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite) :> Stream
    override this.OpenAppend() = _file.Open(FileMode.Append, FileAccess.Write, FileShare.Read) :> Stream
    override this.Length = _file.Length

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

    override this.Length =
      _buffer.LongLength
