namespace TokiwaDb.Core

open System
open System.Collections.Generic
open System.IO
open System.Text
open HashTableDetail

[<AutoOpen>]
module StorageExtensions =
  type Storage with
    member this.Derefer(recordPtr: RecordPointer): Record =
      recordPtr |> Array.map (fun valuePtr -> this.Derefer(valuePtr))

    member this.Store(record: Record): RecordPointer =
      record |> Array.map (fun value -> this.Store(value))

type SequentialStorage(_source: StreamSource) =
  let _readData stream =
    let len       = stream |> Stream.readInt64
    // TODO: Support "long" array.
    assert (len <= (1L <<< 31))
    let len       = int len
    let data      = Array.zeroCreate len
    let _         = stream.Read(data, 0, len)
    in data

  let _readDataAt p =
    use stream    = _source.OpenRead()
    let _         = stream.Seek(p, SeekOrigin.Begin)
    in _readData stream

  let _writeData (data: array<byte>) =
    use stream    = _source.OpenAppend()
    let p         = stream.Position
    let length    = 8L + data.LongLength
    let ()        = stream |> Stream.writeInt64 data.LongLength
    let ()        = stream.Write(data, 0, data.Length)
    in p

  /// Reads the data written at the current position.
  /// Advances the position by the number of bytes read.
  member this.ReadData(stream: Stream): array<byte> =
    _readData stream

  member this.ReadData(p): array<byte> =
    _readDataAt p
    
  member this.WriteData(data: array<byte>): StoragePointer =
    _writeData data

/// A storage which stores values in stream.
type StreamSourceStorage(_source: StreamSource, _hashTableSource: StreamSource) =
  inherit Storage()

  let _source = SequentialStorage(_source)

  let _hash (xs: array<byte>) = xs |> Array.hash

  let _hashTableElementSerializer =
    { new FixedLengthSerializer<HashTableElement<array<byte>, StoragePointer>>() with
        override this.Serialize(element) =
          let p' =
            match element with
            | Busy (_, p, _) -> p
            | Empty -> -1L
            | Removed -> -2L
          in BitConverter.GetBytes(p')

        override thisSerializer.Deserialize(data) =
          match BitConverter.ToInt64(data, 0) with
          | -1L -> Empty
          | -2L -> Removed
          | p when p >= 0L -> 
            // TODO: Lazy loading.
            let data = _source.ReadData(p)
            in Busy (data, p, _hash data)
          | _ -> failwith "unexpected"

        override this.Length = 8L
    }

  let _hashTable =
    let rootArray =
      StreamArray<HashTableElement<array<byte>, StoragePointer>>
        ( _hashTableSource
        , _hashTableElementSerializer
        )
    in
      HashTable<array<byte>, StoragePointer>(_hash, rootArray)

  let _tryFindData data =
    _hashTable.TryFind(data)

  let _readData (p: StoragePointer) =
    _source.ReadData(p)

  let _writeData data =
    match _tryFindData data with
    | Some p -> p
    | None ->
      let p       = _source.WriteData(data)
      let ()      =  _hashTable.Update(data, p)
      in p

  let _readString p =
    UTF8Encoding.UTF8.GetString(_readData p)

  let _writeString (s: string) =
    UTF8Encoding.UTF8.GetBytes(s) |> _writeData

  member this.HashTableElementSerializer =
    _hashTableElementSerializer

  member this.TryFindData(data) =
    _tryFindData data

  member this.WriteData(data) =
    _writeData data

  override this.Derefer(valuePtr): Value =
    match valuePtr with
    | PInt x       -> Int x
    | PFloat x     -> Float x
    | PTime x      -> Time x
    | PString p    -> p |> _readString |> Value.String

  override this.Store(value) =
    match value with
    | Int x       -> PInt x
    | Float x     -> PFloat x
    | Time x      -> PTime x
    | String s    -> s |> _writeString |> PString

type MemoryStorage() =
  inherit StreamSourceStorage(new MemoryStreamSource(), new MemoryStreamSource())
