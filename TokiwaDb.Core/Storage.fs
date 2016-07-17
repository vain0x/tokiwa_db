namespace TokiwaDb.Core

open System
open System.Collections.Generic
open System.IO
open System.Text
open TokiwaDb.Core.FsSerialize
open TokiwaDb.Core.FsSerialize.TypeDefinitions.Custom
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

module StreamStorageDetail =
  type HashTableElement = HashTableElement<array<byte>, StoragePointer>

open StreamStorageDetail

/// A storage which stores values in stream.
type StreamSourceStorage(_source: StreamSource, _hashTableSource: StreamSource) =
  inherit Storage()

  let _source = SequentialStorage(_source)

  let _readData (p: StoragePointer) =
    _source.ReadData(p)

  let _hash (xs: array<byte>) = xs |> Array.hash

  let _hashTable =
    let elementTypeDefinition =
      ofBijective
        ((=) typeof<HashTableElement>)
        { new Bijective<HashTableElement, int64>() with
            override this.Convert(value) =
              match value with
              | Busy (_, p, _)  -> p
              | Empty           -> -1L
              | Removed         -> -2L
            override this.Invert(p) =
              match p with
              | -1L     -> Empty
              | -2L     -> Removed
              | p       ->
                assert (p >= 0L)
                let data        = _readData p
                in Busy(data, p, _hash data)
        }
    let rootArray =
      StreamArray<HashTableElement>(_hashTableSource, [elementTypeDefinition])
    in
      HashTable<array<byte>, StoragePointer>(_hash, rootArray)

  let _tryFindData data =
    _hashTable.TryFind(data)

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
    | PBinary p    -> p |> _readData |> Value.Binary

  override this.Store(value) =
    match value with
    | Int x       -> PInt x
    | Float x     -> PFloat x
    | Time x      -> PTime x
    | String s    -> s |> _writeString |> PString
    | Binary data -> data |> _writeData |> PBinary

type MemoryStorage() =
  inherit StreamSourceStorage(new MemoryStreamSource(), new MemoryStreamSource())
