namespace TokiwaDb.Core

open System
open System.IO

type StreamTable(_db: Database, _schema: TableSchema, _indexes: array<HashTableIndex>, _recordPointersSource: StreamSource) =
  inherit Table()

  let _fields =
    _schema |> TableSchema.toFields

  let mutable _indexes =
    _indexes

  let _recordLength =
    // Count of fields + begin/end revision ids - record id.
    (_fields.LongLength + 2L - 1L) * 8L

  let mutable _hardLength =
    _recordPointersSource.Length / _recordLength

  let _insertedRecordsInTransaction () =
    _db.Transaction.Operations |> Seq.collect (fun operation ->
      seq {
        match operation with
        | InsertRecords (tableName, records) when tableName = _schema.Name ->
          yield! records
        | _ -> ()
      })

  let _length () =
    let countScheduledInserts =
      _insertedRecordsInTransaction () |> Seq.length |> int64
    in _hardLength + countScheduledInserts

  let _positionFromId (recordId: Id) =
    if 0L <= recordId && recordId < _length ()
    then recordId * _recordLength |> Some
    else None

  /// Read a record written at the current position.
  /// NOTE: The value of ID isn't written.
  let _readRecordPointer (stream: Stream) =
    let beginRevision   = stream |> Stream.readInt64
    let endRevision     = stream |> Stream.readInt64
    let recordPointer   =
      [|
        yield (stream.Position / _recordLength |> PInt)
        for Field (_, type') in _fields |> Seq.skip 1 do
          yield stream |> Stream.readInt64 |> ValuePointer.ofUntyped type'
      |]
    in
      {
        Begin     = beginRevision
        End       = endRevision
        Value     = recordPointer
      }

  let _writeRecordPointer t recordPointer (stream: Stream) =
    let recordPointer = recordPointer |> RecordPointer.dropId
    stream |> Stream.writeInt64 t
    stream |> Stream.writeInt64 Mortal.maxLifeSpan
    for valuePointer in recordPointer do
      stream |> Stream.writeInt64 (valuePointer |> ValuePointer.toUntyped)

  /// Set to `t` the end of lifespan of the record written at the current position.
  /// Advances the position to the next record.
  let _kill t (stream: Stream) =
    stream.Seek(8L, SeekOrigin.Current) |> ignore
    stream |> Stream.writeInt64 t
    stream.Seek(_recordLength - 16L, SeekOrigin.Current) |> ignore

  let _allRecordPointers () =
    seq {
      let stream = _recordPointersSource.OpenRead()
      let length = _length ()
      for recordId in 0L..(length - 1L) do
        yield stream |> _readRecordPointer
    }

  let _aliveRecordPointers (t: RevisionId) =
    seq {
      for rp in _allRecordPointers () do
        if rp |> Mortal.isAliveAt t then
          yield rp.Value
    }

  /// Each of recordPointers doesn't contain id field.
  let _validateUniqueness (recordPointers: array<RecordPointer>) =
    recordPointers |> Array.partitionMap (fun rp ->
      _indexes |> Array.tryPick (fun index ->
        let rp = Array.append [| PInt -1L |] rp
        match index.TryFind(index.Projection(rp)) with
        | Some _ as self -> self
        | None ->
          _insertedRecordsInTransaction ()
          |> Seq.tryPick (fun insertedRp ->
            if index.Projection(insertedRp) = index.Projection(rp)
            then insertedRp |> RecordPointer.tryId
            else None
            ))
      |> Option.map (fun recordId ->
        Error.DuplicatedRecord (rp, recordId)
        ))

  new (db, schema, source) =
    StreamTable(db, schema, [||], source)

  override this.Schema = _schema

  override this.Indexes = _indexes

  override this.Relation(t) =
    NaiveRelation(_fields, _aliveRecordPointers t) :> Relation

  override this.ToSeq() =
    _allRecordPointers ()

  override this.RecordById(recordId: Id) =
    _positionFromId recordId |> Option.map (fun position ->
      use stream    = _recordPointersSource.OpenRead()
      let _       = stream.Seek(position, SeekOrigin.Begin)
      in stream |> _readRecordPointer
      )

  override this.Database = _db

  override this.PerformInsert(recordPointers: array<RecordPointer>) =
    let revId           = _db.Transaction.RevisionServer.Next
    use stream          = _recordPointersSource.OpenAppend()
    let ()              =
      for rp in recordPointers do
        for index in _indexes do
          index.Insert(index.Projection(rp), _hardLength)
        stream |> _writeRecordPointer revId rp
        _hardLength <- _hardLength + 1L
    in ()

  override this.Insert(records: array<Record>) =
    let (errors, records) =
      // Validate the length of the record.
      // TODO: Runtime type validation.
      records |> Array.partitionMap (fun record ->
        if record.Length + 1 <> _fields.Length then
          Error.WrongFieldsCount (_schema.Fields, record) |> Some
        else None
        )
    let recordPointers =
      records |> Array.map (fun record -> _db.Storage.Store(record))
    let (errors, recordPointers) =
      // Validate uniqueness.
      _validateUniqueness recordPointers
      |> (fun (duplicationErrors, recordPointers) -> 
        (Array.append errors duplicationErrors, recordPointers)
        )
    let length = _length ()
    let recordPointers =
      // Add index the new records.
      recordPointers |> Array.mapi (fun i rp -> Array.append [| PInt (length + int64 i) |] rp)
    let () =
      if errors |> Array.isEmpty then
        _db.Transaction.Add(InsertRecords (this.Name, recordPointers))
    in errors

  override this.PerformRemove(recordIds) =
    use stream    = _recordPointersSource.OpenReadWrite()
    let revId     = _db.Transaction.RevisionServer.Next
    let ()        =
      for recordId in recordIds |> Array.sort do
        match _positionFromId recordId with
        | None -> ()
        | Some position ->
          let _         = stream.Seek(position, SeekOrigin.Begin)
          let record    = stream |> _readRecordPointer
          in
            if (record |> Mortal.isAliveAt revId) then
              stream.Seek(-_recordLength, SeekOrigin.Current) |> ignore
              stream |> _kill revId
              for index in _indexes do
                index.Remove(index.Projection(record.Value)) |> ignore
    in ()

  override this.Remove(recordIds) =
    let (recordIds, invalidIds) =
      recordIds |> Array.partition (fun recordId -> _positionFromId recordId |> Option.isSome)
    let errors =
      invalidIds |> Array.map Error.InvalidId
    let () =
      _db.Transaction.Add(RemoveRecords (this.Name, recordIds))
    in errors
