namespace TokiwaDb.CodeFirst.Detail

open System
open System.Reflection
open TokiwaDb.Core
open TokiwaDb.CodeFirst

module DbContext =
  let isTableType typ =
    typ |> Type.isGenericTypeDefOf typedefof<Table<_>>

  let tableProperties =
    let bindingFlags =
          BindingFlags.DeclaredOnly
      ||| BindingFlags.Instance
      ||| BindingFlags.Public
    fun (contextType: Type) ->
      contextType.GetProperties(bindingFlags)
      |> Array.filter (fun pi -> pi.PropertyType |> isTableType)

  let modelTypes contextType =
    contextType
    |> tableProperties
    |> Array.map (fun pi -> pi.PropertyType.GetGenericArguments().[0])

  let construct<'c> (db: Database) (findTable: Type -> obj): 'c =
    let contextType     = typeof<'c>
    let tableTypes      = contextType |> tableProperties |> Array.map (fun pi -> pi.PropertyType)
    let modelTypes      = contextType |> modelTypes
    let constructorArgumentTypes =
      [|
        yield typeof<Database>
        yield! tableTypes
      |]
    let constructArguments =
      [|
        yield db :> obj
        yield! modelTypes |> Array.map findTable
      |]
    in
      contextType
        .GetConstructor(constructorArgumentTypes)
        .Invoke(constructArguments)
      :?> 'c
