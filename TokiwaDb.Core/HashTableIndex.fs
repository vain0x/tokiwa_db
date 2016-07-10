namespace TokiwaDb.Core

open TokiwaDb.Core.FsSerialize.TypeDefinitions
open HashTableDetail

type StreamHashTableIndex(_fieldIndexes: array<int>, _source: StreamSource) =
  inherit ImplHashTableIndex()

  let _hashTable =
    let fixedLengthArrayTypeDefinition =
      Array.fixedLengthArrayTypeDefinition _fieldIndexes.LongLength
    let rootArray =
      StreamArray(_source, [fixedLengthArrayTypeDefinition])
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
