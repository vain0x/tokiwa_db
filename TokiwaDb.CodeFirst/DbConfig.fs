namespace TokiwaDb.CodeFirst

open System
open System.Collections.Generic
open System.IO
open TokiwaDb.Core
open TokiwaDb.CodeFirst.Detail

type DbConfig() =
  let mutable _tableSchemas = Dictionary<Type, TableSchema>()

  let _addTable modelType itss =
    match _tableSchemas |> IDictionary.tryFind modelType with
    | Some _ ->
      ArgumentException(sprintf "ƒe[ƒuƒ‹ %s ‚Í‚·‚Å‚É“o˜^‚³‚ê‚Ä‚¢‚Ü‚·B(Table '%s' has been already registered.)" modelType.Name modelType.Name) |> raise
    | None ->
      _tableSchemas.Add(modelType, TableSchema.ofModel modelType |> TableSchema.alter itss)
    
  let _open implDb =
    new OrmDatabase(implDb, _tableSchemas |> IDictionary.toPairSeq)
    
  member this.AddTable<'m when 'm :> IModel>([<ParamArray>] itss: IIncrementalTableSchema []) =
    _addTable typeof<'m> itss

  member this.OpenMemory(name: string) =
    new MemoryDatabase(name) |> _open

  member this.OpenDirectory(dir: DirectoryInfo) =
    new DirectoryDatabase(dir) |> _open
