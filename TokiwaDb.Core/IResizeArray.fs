namespace TokiwaDb.Core

open System
open System.Collections.Generic
open System.IO

type IResizeArray<'x> =
  abstract member Length: int64
  abstract member Get: int64 -> 'x
  abstract member Set: int64 * 'x -> unit
  abstract member Initialize: int64 * 'x -> unit

/// A serializer which can serialize a value to a byte array with fixed length;
/// and can deserialize the byte array to the original value.
type [<AbstractClass>] FixedLengthSerializer<'x>() =
  abstract member Serialize: 'x -> array<byte>
  abstract member Deserialize: array<byte> -> 'x
  abstract member Length: int64

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module IResizeArray =
  let toSeq (xs: IResizeArray<'x>): seq<'x> =
    seq { for i in 0L..(xs.Length - 1L) -> xs.Get(i) }

/// A stream source as an array of elements which can be serialized in fixed length.
type StreamArray<'x>
  ( _source: IStreamSource
  , _serializer: FixedLengthSerializer<'x>
  ) =
  let _length () =
    _source.Length / _serializer.Length

  interface IResizeArray<'x> with
    override this.Length =
      _length ()

    override this.Initialize(length: int64, x: 'x) =
      let data      = _serializer.Serialize(x)
      let ()        = _source.Clear()
      use stream    = _source.OpenReadWrite()
      let ()        =
        for i in 0L..(length - 1L) do
          stream.Write(data, 0, data.Length)
      in ()

    override this.Get(i: int64): 'x =
      if 0L <= i && i < _length () then
        let buffer    = Array.zeroCreate (int _serializer.Length)
        use stream    = _source.OpenRead()
        let _         = stream.Seek(i * _serializer.Length, SeekOrigin.Begin)
        let _         = stream.Read(buffer, 0, buffer.Length)
        in _serializer.Deserialize(buffer)
      else
        raise (ArgumentException())

    override this.Set(i: int64, x: 'x) =
      if 0L <= i && i < _length () then
        let data      = _serializer.Serialize(x)
        use stream    = _source.OpenReadWrite()
        let _         = stream.Seek(i * _serializer.Length, SeekOrigin.Begin)
        in stream.Write(data, 0, data.Length)
      else
        raise (ArgumentException())
