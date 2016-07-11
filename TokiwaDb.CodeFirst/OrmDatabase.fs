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

  let _typeFromTableName =
    _tableSchemas |> Seq.map (fun (type', schema) -> (schema.Name, type'))
    |> Map.ofSeq

  let _implTableFromType: IDictionary<Type, ImplTable> =
    seq {
      for table in _livingTables () do
        match _typeFromTableName |> Map.tryFind table.Name with
        | Some type' -> yield (type', table)
        | None -> ()
    } |> dict

  let _tableFromType modelType: obj =
    match _implTableFromType.TryGetValue(modelType) with
    | (true, implTable) ->
      let tableType     = typedefof<OrmTable<_>>.MakeGenericType(modelType)
      in Activator.CreateInstance(tableType, [| implTable :> obj |])
    | (false, _) -> ArgumentException() |> raise

  member this.FindTable(modelType) =
    _tableFromType modelType

  override this.Dispose() =
    _impl.Dispose()

  override this.Name =
    _impl.Name

  override this.CurrentRevisionId =
    _impl.CurrentRevisionId

  override this.Transaction =
    _impl.Transaction

  override this.Table<'m when 'm :> IModel>() =
    _tableFromType typeof<'m> :?> Table<'m>
