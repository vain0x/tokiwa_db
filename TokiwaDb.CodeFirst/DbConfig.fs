namespace TokiwaDb.CodeFirst

open System
open System.IO
open TokiwaDb.Core
open TokiwaDb.CodeFirst.Detail

type DbConfig() =
  let mutable _tableSchemas = ResizeArray<Type * TableSchema>()

  let _open implDb =
    new OrmDatabase(implDb, _tableSchemas)
    
  member this.AddTable<'m when 'm :> IModel>([<ParamArray>] itss: IIncrementalTableSchema []) =
    _tableSchemas.Add((typeof<'m>, TableSchema.ofModel typeof<'m> |> TableSchema.alter itss))

  member this.OpenMemory(name: string) =
    new MemoryDatabase(name) |> _open

  member this.OpenDirectory(dir: DirectoryInfo) =
    new DirectoryDatabase(dir) |> _open
