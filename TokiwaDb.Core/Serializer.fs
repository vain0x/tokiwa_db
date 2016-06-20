namespace TokiwaDb.Core

/// A serializer which can serialize a value to a byte array with fixed length;
/// and can deserialize the byte array to the original value.
type [<AbstractClass>] FixedLengthSerializer<'x>() =
  abstract member Serialize: 'x -> array<byte>
  abstract member Deserialize: array<byte> -> 'x
  abstract member Length: int64
