namespace TokiwaDb.Core

open System
open System.Collections
open System.Collections.Generic
open System.IO
open TokiwaDb.Core.FsSerialize.Public
open TokiwaDb.Core.FsSerialize.TypeDefinitions.Custom

type IResizeArray<'x> =
  inherit IEnumerable<'x>

  abstract member Length: int64
  abstract member Get: int64 -> 'x
  abstract member Set: int64 * 'x -> unit
  abstract member Initialize: int64 * 'x -> unit
  abstract member SetAll: (IResizeArray<'x> -> unit) -> unit

/// A stream source as an array of elements which can be serialized in fixed length.
type StreamArray<'x>(_source: StreamSource, _customDefinitions: list<TypeDefinition>) =
  let _unitSize =
    (serializedLengthWith<'x> _customDefinitions) |> FsSerialize.Length.toInt64

  let _length () =
    _source.Length / _unitSize

  let _initialize length (x: 'x) =
    let writeTo (source: StreamSource) =
      use stream    = source.OpenReadWrite()
      in
        for i in 0L..(length - 1L) do
          stream |> Stream.serializeWith<'x> _customDefinitions x
    in
      _source.WriteAll(writeTo)

  let _get i =
    if 0L <= i && i < _length () then
      let buffer    = Array.zeroCreate (int _unitSize)
      use stream    = _source.OpenRead()
      let _         = stream.Seek(i * _unitSize, SeekOrigin.Begin)
      in stream |> Stream.deserializeWith<'x> _customDefinitions
    else
      raise (ArgumentException())

  let _toSeq () =
    seq {
      let length      = _length ()
      let stream      = _source.OpenRead()
      for i in 0L..(length - 1L) do
        yield stream |> Stream.deserializeWith<'x> _customDefinitions
      do stream.Dispose()
    }

  let _set i (x: 'x) =
    if 0L <= i && i < _length () then
      use stream    = _source.OpenReadWrite()
      let _         = stream.Seek(i * _unitSize, SeekOrigin.Begin)
      in stream |> Stream.serializeWith<'x> _customDefinitions x
    else
      raise (ArgumentException())

  let _setAll initializer =
    let write source =
      StreamArray(source, _customDefinitions) |> initializer
    in
      _source.WriteAll(write)

  new(source) =
    StreamArray(source, [])

  interface IResizeArray<'x> with
    override this.Length =
      _length ()

    override this.Initialize(length: int64, x: 'x) =
      _initialize length x

    override this.Get(i: int64): 'x =
      _get i

    override this.Set(i: int64, x: 'x) =
      _set i x

    override this.SetAll(initializer) =
      _setAll initializer

    override this.GetEnumerator() =
      (_toSeq () :> seq<'x>).GetEnumerator()

    override this.GetEnumerator() =
      (_toSeq() :> IEnumerable).GetEnumerator()
