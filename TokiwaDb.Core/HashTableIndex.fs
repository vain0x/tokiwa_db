namespace TokiwaDb.Core

open HashTableDetail

type StreamHashTableIndex(_fieldIndexes: array<int>, _source: StreamSource) =
  inherit ImplHashTableIndex()

  let _hashTable =
    let serializer =
      HashTableElementSerializer<RecordPointer, RecordId>
        ( RecordPointer.serializer _fieldIndexes.LongLength
        , Int64Serializer()
        )
    let rootArray =
      StreamArray(_source, serializer)
    in
      HashTable<RecordPointer, RecordId>(RecordPointer.hash, rootArray)

  member this.FieldIndexes =
    _fieldIndexes

  override this.Projection (recordPointer: RecordPointer) =
    _fieldIndexes |> Array.map (fun i -> recordPointer.[i])

  override this.TryFind(recordPointer: RecordPointer): option<RecordId> =
    _hashTable.TryFind(recordPointer)

  override this.Insert(recordPointer: RecordPointer, recordId: RecordId) =
    _hashTable.Update(recordPointer, recordId)

  override this.Remove(recordPointer: RecordPointer) =
    _hashTable.Remove(recordPointer)
