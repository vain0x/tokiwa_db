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
      MultiHashTable<RecordPointer, RecordId>(RecordPointer.hash, rootArray)

  member this.FieldIndexes =
    _fieldIndexes

  override this.Projection (recordPointer: RecordPointer) =
    _fieldIndexes |> Array.map (fun i -> recordPointer.[i])

  override this.FindAll(recordPointer: RecordPointer) =
    _hashTable.FindAll(recordPointer)

  override this.Insert(recordPointer: RecordPointer, recordId: RecordId) =
    _hashTable.Insert(recordPointer, recordId)
