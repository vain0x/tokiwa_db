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

  let _length (stream: Stream) =
    stream.Length / _recordLength

  let _positionFromId (recordId: Id) (stream: Stream) =
    if 0L <= recordId && recordId < (_length stream)
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
      let length = _length stream
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
    use stream    = _recordPointersSource.OpenRead()
    in
      _positionFromId recordId stream |> Option.map (fun position ->
        let _       = stream.Seek(position, SeekOrigin.Begin)
        in stream |> _readRecordPointer
        )

  override this.Database = _db

  override this.Insert(records: array<Record>) =
    let rev = _db.RevisionServer
    /// Add auto-increment field.
    lock _db.SyncRoot (fun () ->
      let _               = rev.Next()
      use stream          = _recordPointersSource.OpenAppend()
      let nextId          = _length stream |> ref
      in
        [|
          for record in records do
            /// TODO: Runtime type validation.
            if record.Length + 1 <> _fields.Length then
              yield Error.WrongFieldsCount (_schema.Fields, record)
            else
              let recordPointer =
                Array.append [| PInt (! nextId) |] (_db.Storage.Store(record))
              for index in _indexes do
                index.Insert(index.Projection(recordPointer), ! nextId)
              stream |> _writeRecordPointer rev.Current recordPointer
              nextId := (! nextId) + 1L
        |])

  override this.Remove(recordIds) =
    lock _db.SyncRoot (fun () ->
      let rev       = _db.RevisionServer
      let revId     = rev.Next()
      let recordIds = recordIds |> Array.sort
      use stream    = _recordPointersSource.OpenReadWrite()
      in
        [|
          for recordId in recordIds do
            match _positionFromId recordId stream with
            | None ->
              yield Error.InvalidId recordId
            | Some position ->
              let _     = stream.Seek(position, SeekOrigin.Begin)
              let record = stream |> _readRecordPointer
              in
                if (record |> Mortal.isAliveAt revId) then
                  stream.Seek(-_recordLength, SeekOrigin.Current) |> ignore
                  stream |> _kill revId
                  for index in _indexes do
                    index.Remove(index.Projection(record.Value)) |> ignore
        |])
