namespace TokiwaDb.CodeFirst

open System
open System.Collections.Generic
open System.IO
open TokiwaDb.Core
open TokiwaDb.CodeFirst.Detail

type DbConfig<'c>() =
  let _modelTypes = typeof<'c> |> DbContext.modelTypes

  let mutable _tableSchemas =
    _modelTypes
    |> Seq.map (fun modelType -> (modelType, TableSchema.ofModel modelType))
    |> Dictionary.ofSeq

  let _findSchema modelType =
    _tableSchemas |> IDictionary.tryFind modelType
    |> Option.getOrElse (fun () ->
      ArgumentException
        ( sprintf "モデルクラス '%s' はデータベース文脈クラス '%s' に登録されていません。(Model class '%s' hasn't been registered in database context class '%s'.)"
            modelType.Name typeof<'c>.Name
            modelType.Name typeof<'c>.Name
        ) |> raise
      )

  let _add modelType itss =
    _tableSchemas.[modelType] <- _findSchema modelType |> TableSchema.alter itss

  let _createDatabase implDb =
    new OrmDatabase(implDb, _tableSchemas |> IDictionary.toPairSeq)

  let _open implDb =
    let db      = _createDatabase implDb
    in DbContext.construct<'c> db (fun typ -> db.FindTable(typ))
    
  member this.Add<'m when 'm :> IModel>([<ParamArray>] itss: IIncrementalTableSchema []) =
    _add typeof<'m> itss

  member this.OpenMemory(name: string) =
    new MemoryDatabase(name) |> _open

  member this.OpenDirectory(dir: DirectoryInfo) =
    new DirectoryDatabase(dir) |> _open
