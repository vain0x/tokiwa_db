namespace TokiwaDb.CodeFirst

open System
open System.IO
open TokiwaDb.Core
open TokiwaDb.CodeFirst.Detail

type DbConfig() =
  let mutable _tableSchemas = ResizeArray<Type * TableSchema>()

  member this.AddTable<'m when 'm :> IModel>([<ParamArray>] itss: IIncrementalTableSchema []) =
    _tableSchemas.Add((typeof<'m>, TableSchema.ofModel<'m> () |> TableSchema.alter itss))

  member this.OpenMemory(name: string) =
    new OrmDatabase(new MemoryDatabase(name), _tableSchemas)

  member this.OpenDirectory(dir: DirectoryInfo) =
    new OrmDatabase(new DirectoryDatabase(dir), _tableSchemas)
