namespace TokiwaDb.Core

open System
open System.IO
open Chessie.ErrorHandling

type RepositoryTable(_db: ImplDatabase, _id: TableId, _repo: Repository) =
  inherit ImplTable()

  let mutable _schema =
    (_repo.TryFind(".schema") |> Option.get).ReadString()
    |> FsYaml.customLoad<TableSchema>

  let _indexes =
    _schema.Indexes |> Array.mapi (fun i indexSchema ->
      match indexSchema with
      | HashTableIndexSchema fieldIndexes ->
        _repo.TryFind(sprintf "%d.ht_index" i)
        |> Option.get
        |> (fun source ->
          StreamHashTableIndex(fieldIndexes, source) :> ImplHashTableIndex
          ))

  let _recordPointersSource =
    _repo.TryFind(".table") |> Option.get

  let _fields =
    _schema |> TableSchema.toFields

  let _isAliveAt t =
    _schema.LifeSpan |> Mortal.isAliveAt t

  let _recordLength =
    // Count of fields + begin/end revision ids - record id.
    (_fields.LongLength + 2L - 1L) * 8L

  let mutable _hardLength =
    _recordPointersSource.Length / _recordLength

  let _transaction () =
    _db.ImplTransaction

  let _canBeModified () =
    let alreadyDropped () =
      _isAliveAt _db.CurrentRevisionId |> not
    let willBeDropped () =
      (_transaction ()).Operations |> Seq.exists (fun operation ->
        match operation with
        | DropTable (tableId) when tableId = _id -> true
        |_ -> false
        )
    in not (alreadyDropped ()) && not (willBeDropped ())

  let _insertedRecordsInTransaction () =
    (_transaction ()).Operations |> Seq.collect (fun operation ->
      seq {
        match operation with
        | InsertRecords (tableId, records) when tableId = _id ->
          yield! records
        | _ -> ()
      })

  let _length () =
    let countScheduledInserts =
      _insertedRecordsInTransaction () |> Seq.length |> int64
    in _hardLength + countScheduledInserts

  let _positionFromId (recordId: RecordId): option<int64> =
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

  let _recordById recordId =
    _positionFromId recordId |> Option.map (fun position ->
      use stream    = _recordPointersSource.OpenRead()
      let _       = stream.Seek(position, SeekOrigin.Begin)
      in stream |> _readRecordPointer
      )

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

  let _performInsert recordPointers =
    let revId           = (_transaction ()).RevisionServer.Next
    use stream          = _recordPointersSource.OpenAppend()
    let ()              =
      for rp in recordPointers do
        for index in _indexes do
          index.Insert(index.Projection(rp), _hardLength)
        stream |> _writeRecordPointer revId rp
        _hardLength <- _hardLength + 1L
    in ()

  let _insert records =
    lock (_transaction ()).SyncRoot (fun () ->
      trial {
        if _canBeModified () |> not then
          do! fail <| Error.TableAlreadyDropped _id
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
          (_transaction ()).Add(InsertRecords (_id, recordPointers))
        return recordIds
      })

  let _performRemove recordIds =
    use stream    = _recordPointersSource.OpenReadWrite()
    let revId     = (_transaction ()).RevisionServer.Next
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

  let _remove recordIds =
    trial {
      if _canBeModified () |> not then
        do! fail <| Error.TableAlreadyDropped _id
      let! recordIds =
        [|
          for recordId in recordIds ->
            trial {
              let! (_: int64) =
                recordId |> _positionFromId |> failIfNone (Error.InvalidRecordId recordId)
              return recordId
            }
        |]
        |> Trial.collect
      let () =
        (_transaction ()).Add(RemoveRecords (_id, recordIds |> List.toArray))
      return ()
    }

  let _performDrop () =
    let revisionId      = (_transaction ()).RevisionServer.Next
    let ()              =
      _schema <- { _schema with LifeSpan = _schema.LifeSpan |> Mortal.kill revisionId }
    let ()              =
      (_repo.TryFind(".schema") |> Option.get)
        .WriteString(_schema |> FsYaml.customDump)
    in ()

  let _drop () =
    if _canBeModified () then
      (_transaction ()).Add(DropTable _id)

  static member Create
    ( db: ImplDatabase
    , tableId: TableId
    , repo: Repository
    , schema: TableSchema
    , revisionId: RevisionId
    ) =
    // Get born.
    let schema          = { schema with LifeSpan = schema.LifeSpan |> Mortal.isBorn revisionId }
    /// Create schema file.
    let schemaSource    = repo.Add(".schema")
    let ()              = schemaSource.WriteString(schema |> FsYaml.customDump)
    /// Create index files.
    let indexes         =
      schema.Indexes |> Array.mapi (fun i indexSchema ->
        match indexSchema with
        | HashTableIndexSchema fieldIndexes ->
          let indexSource   = repo.Add(sprintf "%d.ht_index" i)
          in StreamHashTableIndex(fieldIndexes, indexSource) :> HashTableIndex
        )
    /// Create table file.
    let tableSource     = repo.Add(".table")
    RepositoryTable(db, tableId, repo)

  override this.Id = _id

  override this.Schema = _schema

  override this.Name = _schema.Name

  override this.Indexes = _indexes

  override this.Relation(t) =
    NaiveRelation(_fields, _aliveRecordPointers t) :> Relation

  override this.RecordPointers =
    _allRecordPointers ()

  override this.RecordById(recordId) =
    _recordById recordId

  override this.Database = _db

  override this.PerformInsert(recordPointers) =
    _performInsert recordPointers

  override this.Insert(records: array<Record>) =
    _insert records

  override this.PerformRemove(recordIds) =
    _performRemove recordIds

  override this.Remove(recordIds) =
    _remove recordIds

  override this.PerformDrop() =
    _performDrop ()

  override this.Drop() =
    _drop ()
