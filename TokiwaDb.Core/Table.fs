namespace TokiwaDb.Core

open System
open System.IO
open Chessie.ErrorHandling

type StreamTable(_db: Database, _id: Id, _schema: TableSchema, _indexes: array<HashTableIndex>, _recordPointersSource: StreamSource) =
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

  let _positionFromId (recordId: Id): option<pointer> =
    if 0L <= recordId && recordId < _length ()
    then recordId * _recordLength |> Some
    else None

  /// Read a record written at the current position.
  /// NOTE: The value of ID isn't written.
  let _readRecordPointer (stream: Stream) =
    let recordId        = stream.Position / _recordLength
    let readValue stream =
      [|
        yield PInt recordId
        yield! RecordPointer.readFromStream (_fields |> Seq.skip 1) stream
      |]
    in
      Mortal.readFromStream readValue stream

  let _writeRecordPointer t recordPointer (stream: Stream) =
    Mortal.create t (recordPointer |> RecordPointer.dropId)
    |> Mortal.writeToStream (fun rp stream -> rp |> RecordPointer.writeToStream stream) stream

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

  let _validateRecordType (record: Record) =
    trial {
      if record |> Record.toType <> (_fields.[1..] |> Array.map Field.toType) then
        return! fail <| Error.WrongRecordType (_schema.Fields, record)
      return record
    }

  /// Each of recordPointers doesn't contain id field.
  let _validateUniqueness1
      (bucket: Set<RecordPointer>)
      (index: HashTableIndex)
      (recordPointer: RecordPointer)
    =
    let rp = Array.append [| PInt -1L |] recordPointer
    let part = index.Projection(rp)
    let isDuplicated =
      index.TryFind(part) |> Option.isSome
      || (_insertedRecordsInTransaction ()
        |> Seq.exists (fun insertedRp -> index.Projection(insertedRp) = part))
      || bucket |> Set.contains part
    in
      if isDuplicated
      then fail (Error.DuplicatedRecord recordPointer)
      else pass part

  let _validateUniqueness (recordPointers: array<RecordPointer>) =
    [
      for index in _indexes do
        let bucket = Set.empty |> ref
        for recordPointer in recordPointers do
          match _validateUniqueness1 (! bucket) index recordPointer with
          | Pass part ->
            bucket := (! bucket) |> Set.add part
          | Warn (_, msgs)
          | Fail msgs ->
            yield! msgs
    ]
    |> (function
      | [] -> Trial.pass ()
      | _ as es -> Bad es)

  let _validateInsertedRecords (records: array<Record>) =
    trial {
      let! records =
        records |> Array.map _validateRecordType
        |> Trial.collect
      let recordPointers =
        records |> List.toArray
        |> Array.map (fun record -> _db.Storage.Store(record))
      do! _validateUniqueness recordPointers
      return recordPointers
    }

  new (db, tableId, schema, source) =
    StreamTable(db, tableId, schema, [||], source)

  override this.Id = _id

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
    lock _db.Transaction.SyncRoot (fun () ->
      trial {
        let! recordPointers = _validateInsertedRecords records
        let length = _length ()
        let (recordIds, recordPointers) =
          // Give indexes to the new records.
          recordPointers
          |> Array.mapi (fun i rp ->
            let recordId = length + int64 i
            in (recordId, Array.append [| PInt recordId |] rp)
            )
          |> Array.unzip
        let () =
          _db.Transaction.Add(InsertRecords (this.Name, recordPointers))
        return recordIds
      })

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
              Mortal.killInStream revId stream
              stream.Seek(_recordLength, SeekOrigin.Current) |> ignore
              for index in _indexes do
                index.Remove(index.Projection(record.Value)) |> ignore
    in ()

  override this.Remove(recordIds) =
    trial {
      let! recordIds =
        [|
          for recordId in recordIds ->
            trial {
              let! (_: pointer) =
                recordId |> _positionFromId |> failIfNone (Error.InvalidId recordId)
              return recordId
            }
        |]
        |> Trial.collect
      let () =
        _db.Transaction.Add(RemoveRecords (this.Name, recordIds |> List.toArray))
      return ()
    }
