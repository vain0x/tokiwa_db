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

type SequentialStorage(_src: StreamSource) =
  /// Reads the data written at the current position.
  /// Advances the position by the number of bytes read.
  member this.ReadData(stream: Stream) =
    let len       = stream |> Stream.readInt64
    // TODO: Support "long" array.
    assert (len <= (1L <<< 31))
    let len       = int len
    let data      = Array.zeroCreate len
    let _         = stream.Read(data, 0, len)
    in data

  member this.ReadData(p) =
    use stream    = _src.OpenRead()
    let _         = stream.Seek(p, SeekOrigin.Begin)
    in this.ReadData(stream)
    
  member this.WriteData(data: array<byte>): pointer =
    use stream    = _src.OpenAppend()
    let p         = stream.Position
    let length    = 8L + data.LongLength
    let ()        = stream |> Stream.writeInt64 data.LongLength
    let ()        = stream.Write(data, 0, data.Length)
    in p

/// A storage which stores values in stream.
type StreamSourceStorage(_src: StreamSource, _hashTableSource: StreamSource) =
  inherit Storage()

  let _src = SequentialStorage(_src)

  let _hash (xs: array<byte>) = xs |> Array.hash

  let _hashTableElementSerializer =
    { new FixedLengthSerializer<HashTableElement<array<byte>, pointer>>() with
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
            let data = _src.ReadData(p)
            in Busy (data, p, _hash data)
          | _ -> failwith "unexpected"

        override this.Length = 8L
    }

  let _hashTable =
    let rootArray =
      StreamArray<HashTableElement<array<byte>, pointer>>
        ( _hashTableSource
        , _hashTableElementSerializer
        )
    in
      HashTable<array<byte>, pointer>(_hash, rootArray)

  member this.HashTableElementSerializer =
    _hashTableElementSerializer

  member this.TryFindData(data: array<byte>) =
    _hashTable.TryFind(data)

  member this.ReadData(p: pointer) =
    _src.ReadData(p)

  member this.WriteData(data) =
    match this.TryFindData(data) with
    | Some p -> p
    | None ->
      let p       = _src.WriteData(data)
      let ()      =  _hashTable.Update(data, p)
      in p

  member this.ReadString(p: pointer) =
    UTF8Encoding.UTF8.GetString(this.ReadData(p))

  member this.WriteString(s: string) =
    this.WriteData(UTF8Encoding.UTF8.GetBytes(s))

  override this.Derefer(valuePtr): Value =
    match valuePtr with
    | PInt x       -> Int x
    | PFloat x     -> Float x
    | PTime x      -> Time x
    | PString p    -> this.ReadString(p) |> Value.String

  override this.Store(value) =
    match value with
    | Int x       -> PInt x
    | Float x     -> PFloat x
    | Time x      -> PTime x
    | String s    -> this.WriteString(s) |> PString

type FileStorage(_file: FileInfo) =
  inherit StreamSourceStorage
    ( FileStreamSource(_file)
    , FileStreamSource(FileInfo(_file.FullName + ".hashtable"))
    )

type MemoryStorage() =
  inherit StreamSourceStorage(new MemoryStreamSource(), new MemoryStreamSource())
