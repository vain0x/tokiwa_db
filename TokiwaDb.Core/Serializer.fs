namespace TokiwaDb.Core

/// A serializer which can serialize a value to a byte array with fixed length;
/// and can deserialize the byte array to the original value.
type [<AbstractClass>] FixedLengthSerializer<'x>() =
  abstract member Serialize: 'x -> array<byte>
  abstract member Deserialize: array<byte> -> 'x
  abstract member Length: int64

type FixedLengthArraySerializer<'x>
  ( _serializer: FixedLengthSerializer<'x>
  , _length: int64
  ) =
  inherit FixedLengthSerializer<array<'x>>()

  let _dataLength =
    _serializer.Length * _length

  override this.Length =
    _dataLength

  override this.Serialize(xs) =
    assert (xs.LongLength = _length)
    [|
      for x in xs do
        yield! _serializer.Serialize(x)
    |]

  override this.Deserialize(data) =
    assert (data.LongLength = _dataLength)
    [|
      for i in 0L..(_length - 1L) do
        let p = i * _serializer.Length |> int
        let q = (i + 1L) * _serializer.Length - 1L |> int
        yield _serializer.Deserialize(data.[p..q])
    |]
