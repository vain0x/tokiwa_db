namespace TokiwaDb.Core

open HashTableDetail

type StreamHashTableIndex(_fieldIndexes: array<int>, _source: StreamSource) =
  inherit HashTableIndex()

  let _hashTable =
    let serializer =
      HashTableElementSerializer<RecordPointer, Id>
        ( RecordPointer.serializer _fieldIndexes.LongLength
        , Int64Serializer()
        )
    let rootArray =
      StreamArray(_source, serializer)
    in
      HashTable<RecordPointer, Id>(RecordPointer.hash, rootArray)

  member this.FieldIndexes =
    _fieldIndexes

  override this.Projection (recordPointer: RecordPointer) =
    _fieldIndexes |> Array.map (fun i -> recordPointer.[i])

  override this.TryFind(recordPointer: RecordPointer): option<Id> =
    _hashTable.TryFind(recordPointer)

  override this.Insert(recordPointer: RecordPointer, id: Id) =
    _hashTable.Update(recordPointer, id)

  override this.Remove(recordPointer: RecordPointer) =
    _hashTable.Remove(recordPointer)
