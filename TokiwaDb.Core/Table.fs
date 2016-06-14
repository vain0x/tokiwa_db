namespace TokiwaDb.Core

open System
open System.IO
open FsYaml

type StreamTable(_name: Name, _schemaSource: IStreamSource, _recordPointersSource: IStreamSource) =
  let _schema =
    let yaml =
      use stream = _schemaSource.OpenRead()
      in stream |> Stream.readToEnd
    in Yaml.load<Schema> yaml

  let _fields =
    _schema |> Schema.toFields

  /// Read a record written at the current position.
  let _readRecordPointer (fields: array<Field>) (stream: Stream) =
    let beginRevision   = stream |> Stream.readInt64
    let endRevision     = stream |> Stream.readInt64
    let recordPointer   =
      [|
        for field in fields do
          yield stream |> Stream.readInt64 |> ValuePointer.ofUntyped field 
      |]
    in
      {
        Begin     = beginRevision
        End       = endRevision
        Value     = recordPointer
      }

  let _allRecordPointers () =
    seq {
      let stream = _recordPointersSource.OpenRead()
      while stream.Position < stream.Length do
        yield stream |> _readRecordPointer _fields
    }

  member this.Relation(t) =
    let recordPointers =
      seq {
        for rp in _allRecordPointers () do
          if rp |> Mortal.isAliveAt t then yield rp.Value
      }
    in Relation(_fields, recordPointers)
