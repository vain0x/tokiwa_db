namespace TokiwaDb.Core

open System
open System.Collections
open System.Collections.Generic
open System.IO

type IResizeArray<'x> =
  inherit IEnumerable<'x>

  abstract member Length: int64
  abstract member Get: int64 -> 'x
  abstract member Set: int64 * 'x -> unit
  abstract member Initialize: int64 * 'x -> unit

/// A stream source as an array of elements which can be serialized in fixed length.
type StreamArray<'x>
  ( _source: StreamSource
  , _serializer: FixedLengthSerializer<'x>
  ) =
  let _length () =
    _source.Length / _serializer.Length

  let _initialize length x =
    let data      = _serializer.Serialize(x)
    let ()        = _source.Clear()
    use stream    = _source.OpenReadWrite()
    let ()        =
      for i in 0L..(length - 1L) do
        stream.Write(data, 0, data.Length)
    in ()

  let _get i =
    if 0L <= i && i < _length () then
      let buffer    = Array.zeroCreate (int _serializer.Length)
      use stream    = _source.OpenRead()
      let _         = stream.Seek(i * _serializer.Length, SeekOrigin.Begin)
      let _         = stream.Read(buffer, 0, buffer.Length)
      in _serializer.Deserialize(buffer)
    else
      raise (ArgumentException())

  let _toSeq () =
    seq {
      let buffer      = Array.zeroCreate (int _serializer.Length)
      let length      = _length ()
      let stream      = _source.OpenRead()
      for i in 0L..(length - 1L) do
        let _     = stream.Read(buffer, 0, buffer.Length)
        yield _serializer.Deserialize(buffer)
      do stream.Dispose()
    }

  let _set i x =
    if 0L <= i && i < _length () then
      let data      = _serializer.Serialize(x)
      use stream    = _source.OpenReadWrite()
      let _         = stream.Seek(i * _serializer.Length, SeekOrigin.Begin)
      in stream.Write(data, 0, data.Length)
    else
      raise (ArgumentException())

  interface IResizeArray<'x> with
    override this.Length =
      _length ()

    override this.Initialize(length: int64, x: 'x) =
      _initialize length x

    override this.Get(i: int64): 'x =
      _get i

    override this.Set(i: int64, x: 'x) =
      _set i x

    override this.GetEnumerator() =
      (_toSeq () :> seq<'x>).GetEnumerator()

    override this.GetEnumerator() =
      (_toSeq() :> IEnumerable).GetEnumerator()
