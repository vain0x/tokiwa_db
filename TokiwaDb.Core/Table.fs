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
    (_fields.LongLength + 2L) * 8L

  let mutable _hardLength =
    _recordPointersSource.Length / _recordLength

  let _length () =
    let countScheduledInserts =
      _db.Transaction.Operations |> Seq.sumBy (fun operation ->
        match operation with
        | InsertRecords (tableName, records) when tableName = _schema.Name ->
          records.LongLength
        | _ -> 0L
        )
    in _hardLength + countScheduledInserts

  let _positionFromId (recordId: Id) =
    if 0L <= recordId && recordId < _length ()
    then recordId * _recordLength |> Some
    else None

  /// Read a record written at the current position.
  let _readRecordPointer (stream: Stream) =
    let beginRevision   = stream |> Stream.readInt64
    let endRevision     = stream |> Stream.readInt64
    let recordPointer   =
      [|
        for Field (_, type') in _fields do
          yield stream |> Stream.readInt64 |> ValuePointer.ofUntyped type'
      |]
    in
      {
        Begin     = beginRevision
        End       = endRevision
        Value     = recordPointer
      }

  let _writeRecordPointer t recordPointer (stream: Stream) =
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
        yield (recordId, stream |> _readRecordPointer)
    }

  let _aliveRecordPointers (t: RevisionId) =
    seq {
      for (_, rp) in _allRecordPointers () do
        if rp |> Mortal.isAliveAt t then
          yield rp.Value
    }

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

  override this.PerformInsert(records: array<Record>) =
    lock _db.SyncRoot (fun () ->
      let revId           = _db.RevisionServer.Increase()
      use stream          = _recordPointersSource.OpenAppend()
      let ()              =
        for record in records do
          let recordPointer =
            Array.append [| PInt _hardLength |] (_db.Storage.Store(record))
          for index in _indexes do
            index.Insert(index.Projection(recordPointer), _hardLength)
          stream |> _writeRecordPointer revId recordPointer
          _hardLength <- _hardLength + 1L
      in ())

  override this.Insert(records: array<Record>) =
    let (errors, records) =
      records |> Array.partitionMap (fun record ->
        /// TODO: Runtime type validation.
        if record.Length + 1 <> _fields.Length then
          Error.WrongFieldsCount (_schema.Fields, record) |> Some
        else None
        )
    let () =
      if errors |> Array.isEmpty then
        _db.Transaction.Add(InsertRecords (this.Name, records))
    in errors

  override this.PerformRemove(recordIds) =
    lock _db.SyncRoot (fun () ->
      use stream    = _recordPointersSource.OpenReadWrite()
      let revId     = _db.RevisionServer.Increase()
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
      in ())

  override this.Remove(recordIds) =
    let (recordIds, invalidIds) =
      recordIds |> Array.partition (fun recordId -> _positionFromId recordId |> Option.isSome)
    let errors =
      invalidIds |> Array.map Error.InvalidId
    let () =
      _db.Transaction.Add(RemoveRecords (this.Name, recordIds))
    in errors
