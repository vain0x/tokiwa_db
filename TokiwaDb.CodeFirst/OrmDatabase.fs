namespace TokiwaDb.CodeFirst.Detail

open System
open System.Collections.Generic
open TokiwaDb.Core
open TokiwaDb.CodeFirst

type OrmDatabase(_impl: ImplDatabase, _tableSchemas: seq<Type * TableSchema>) =
  inherit Database()

  let _livingTables () =
    _impl.ImplTables
    |> Seq.filter (Mortal.isAliveAt _impl.CurrentRevisionId)

  let _createTables () =
    for (_, tableSchema) in _tableSchemas do
      _impl.CreateTable(tableSchema) |> ignore

  let _resetIfModelHasChanged () =
    let hasModelChanged () =
      let projection (schema: TableSchema) =
        (schema.Name, schema.Fields)
      let actual =
        _livingTables () |> Seq.map (fun t -> t.Schema |> projection) |> Seq.toList
      let expected =
        _tableSchemas |> Seq.map (snd >> projection) |> Seq.toList
      in actual <> expected
    if hasModelChanged () then
      for t in _livingTables () do
        t.Drop()
      _createTables ()

  do _resetIfModelHasChanged ()

  let _tables: IDictionary<Type, ImplTable> =
    seq {
      let map =
        _tableSchemas |> Seq.map (fun (type', schema) -> (schema.Name, type'))
        |> Map.ofSeq
      for table in _livingTables () do
        match map |> Map.tryFind table.Name with
        | Some type' -> yield (type', table)
        | None -> ()
    } |> dict

  override this.Name =
    _impl.Name

  override this.CurrentRevisionId =
    _impl.CurrentRevisionId

  override this.Transaction =
    _impl.Transaction

  override this.Table<'m when 'm :> IModel>() =
    match _tables.TryGetValue(typeof<'m>) with
    | (true, table) -> OrmTable<'m>(table) :> Table<'m>
    | (false, _) -> ArgumentException() |> raise
