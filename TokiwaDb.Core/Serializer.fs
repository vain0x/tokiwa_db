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

type FixedLengthDoubleSerializer<'x0, 'x1>
  ( _serializer0: FixedLengthSerializer<'x0>
  , _serializer1: FixedLengthSerializer<'x1>
  ) =
  inherit FixedLengthSerializer<'x0 * 'x1>()

  override this.Length =
    _serializer0.Length + _serializer1.Length

  override this.Serialize((x0, x1)) =
    [|
      yield! _serializer0.Serialize(x0)
      yield! _serializer1.Serialize(x1)
    |]

  override this.Deserialize(data) =
    assert (data.LongLength = this.Length)
    let x0 = _serializer0.Deserialize(data.[..(_serializer0.Length - 1L |> int)])
    let x1 = _serializer1.Deserialize(data.[(_serializer0.Length |> int)..])
    in (x0, x1)

type FixedLengthQuadrupleSerializer<'x0, 'x1, 'x2, 'x3>
  ( _serializer0: FixedLengthSerializer<'x0>
  , _serializer1: FixedLengthSerializer<'x1>
  , _serializer2: FixedLengthSerializer<'x2>
  , _serializer3: FixedLengthSerializer<'x3>
  ) =
  inherit FixedLengthSerializer<'x0 * 'x1 * 'x2 * 'x3>()

  let _serializer =
    FixedLengthDoubleSerializer(_serializer0,
      FixedLengthDoubleSerializer(_serializer1,
        FixedLengthDoubleSerializer(_serializer2, _serializer3)))

  override this.Length =
    _serializer.Length

  override this.Serialize((x0, x1, x2, x3)) =
    _serializer.Serialize(x0, (x1, (x2, x3)))

  override this.Deserialize(data) =
    let (x0, (x1, (x2, x3))) = _serializer.Deserialize(data)
    in (x0, x1, x2, x3)
