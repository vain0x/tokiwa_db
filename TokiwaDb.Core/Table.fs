namespace TokiwaDb.Core

open System
open System.IO

type StreamTable(_db: Database, _name: Name, _schema: Schema, _recordPointersSource: IStreamSource) =
  inherit Table()

  let syncRoot = new obj()

  let _fields =
    _schema |> Schema.toFields

  let _recordLength =
    (_fields.LongLength + 2L) * 8L

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
      while stream.Position < stream.Length do
        yield stream |> _readRecordPointer
    }

  let _aliveRecordPointers (t: RevisionId) =
    seq {
      for rp in _allRecordPointers () do
        if rp |> Mortal.isAliveAt t then
          yield rp.Value
    }

  let _rev = _db.RevisionServer

  override this.Name = _name

  override this.Schema = _schema

  override this.Relation(t) =
    NaiveRelation(_fields, _aliveRecordPointers t) :> Relation

  override this.Database = _db

  override this.Insert(recordPointer: RecordPointer) =
    /// Add auto-increment field.
    let recordPointer =
      match _schema.KeyFields with
      | Id ->
        let nextId = _recordPointersSource.Length / _recordLength
        in Array.append [| PInt nextId |] recordPointer
      | _ -> recordPointer
    /// TODO: Runtime type validation.
    assert (recordPointer.Length = _fields.Length)
    lock _db.SyncRoot (fun () ->
      let _         = _rev.Next()
      use stream    = _recordPointersSource.OpenAppend()
      in stream |> _writeRecordPointer _rev.Current recordPointer
      )

  override this.Delete(pred: RecordPointer -> bool) =
    lock _db.SyncRoot (fun () ->
      let _         = _rev.Next() |> ignore
      use stream    = _recordPointersSource.OpenReadWrite()
      while stream.Position < stream.Length do
        let record = stream |> _readRecordPointer
        if (record |> Mortal.isAliveAt _rev.Current) && (record.Value |> pred) then
          stream.Seek(-_recordLength, SeekOrigin.Current) |> ignore
          stream |> _kill _rev.Current
      )
