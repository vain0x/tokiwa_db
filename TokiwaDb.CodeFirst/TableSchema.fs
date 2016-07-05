namespace TokiwaDb.CodeFirst.Detail
  open TokiwaDb.Core
  open TokiwaDb.CodeFirst

  type IIncrementalTableSchema =
    abstract member Alter: TableSchema -> TableSchema

  type [<AbstractClass>] IncrementalIndexSchema() =
    abstract member Schema: IndexSchema

    interface IIncrementalTableSchema with
      override this.Alter(schema) =
        { schema with Indexes = Array.append schema.Indexes [| this.Schema |] }

  module IndexSchema =
    let fieldIndexesFromNames<'m when 'm :> IModel> names =
      let indexFromPropertyName =
        Model.mappedProperties<'m> ()
        |> Array.mapi (fun i prop -> (prop.Name, i + 1))
        |> Map.ofArray
      in
        names |> Array.choosei (fun i name ->
          indexFromPropertyName |> Map.tryFind name
          )

    let fieldIndexesFromLambdas<'m when 'm :> IModel> lambdas =
      lambdas
      |> Array.choose (Expression.tryMemberInfo >> Option.map (fun mi -> mi.Name))
      |> fieldIndexesFromNames<'m>

  module TableSchema =
    let alter (alterers: IIncrementalTableSchema []) schema =
      alterers |> Array.fold (fun schema alterer -> alterer.Alter(schema)) schema

    let ofModel<'m when 'm :> IModel> () =
      { TableSchema.empty typeof<'m>.Name with
          Fields = Model.toFields<'m> ()
      }

namespace TokiwaDb.CodeFirst
  open System
  open System.Linq.Expressions
  open TokiwaDb.Core
  open TokiwaDb.CodeFirst.Detail

  type UniqueIndex(_fieldIndexes: array<int>) =
    inherit IncrementalIndexSchema()

    override this.Schema =
      HashTableIndexSchema _fieldIndexes

    member this.FieldIndexes = _fieldIndexes

    static member Of<'m when 'm :> IModel>([<ParamArray>] names: array<string>) =
      let fieldIndexes =
        names |> IndexSchema.fieldIndexesFromNames<'m>
      in new UniqueIndex(fieldIndexes)

    static member Of<'m when 'm :> IModel>([<ParamArray>] lambdas: array<Expression<Func<'m, obj>>>) =
      let fieldIndexes =
        lambdas |> Array.map (fun l -> l :> Expression)
        |> IndexSchema.fieldIndexesFromLambdas<'m>
      in new UniqueIndex(fieldIndexes)
