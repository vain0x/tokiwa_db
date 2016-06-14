namespace TokiwaDb.Core

open System
open System.IO

type StreamTable(_name: Name, _schema: Schema, _recordPointersSource: IStreamSource) =
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

  interface ITable with
    override this.Name = _name
    override this.Schema = _schema
    override this.Relation(t) =
      Relation(_fields, _aliveRecordPointers t) :> IRelation

    override this.Insert(rs: IRevisionServer, recordPointer: RecordPointer) =
      /// Add auto-increment field.
      let recordPointer =
        match _schema.KeyFields with
        | Id ->
          let nextId = _recordPointersSource.Length / _recordLength
          in Array.append [| PInt nextId |] recordPointer
        | _ -> recordPointer
      /// TODO: Runtime type validation.
      assert (recordPointer.Length = _fields.Length)
      use stream    = _recordPointersSource.OpenAppend()
      let ()        = stream |> _writeRecordPointer rs.Current recordPointer
      in rs.Next() |> ignore

    override this.Delete(rs: IRevisionServer, pred: RecordPointer -> bool) =
      ()
