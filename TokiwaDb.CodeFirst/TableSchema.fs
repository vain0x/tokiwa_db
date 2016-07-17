namespace TokiwaDb.CodeFirst.Detail
  open System
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
    let fieldIndexesFromNames modelType names =
      let indexFromPropertyName =
        Model.mappedProperties modelType
        |> Array.mapi (fun i prop -> (prop.Name, i + 1))
        |> Map.ofArray
      in
        names |> Array.choosei (fun i name ->
          indexFromPropertyName |> Map.tryFind name
          )

    let fieldIndexesFromLambdas modelType lambdas =
      lambdas
      |> Array.choose (Expression.tryMemberInfo >> Option.map (fun mi -> mi.Name))
      |> fieldIndexesFromNames modelType

  module TableSchema =
    let alter (alterers: IIncrementalTableSchema []) schema =
      alterers |> Array.fold (fun schema alterer -> alterer.Alter(schema)) schema

    let ofModel (modelType: Type) =
      { TableSchema.empty modelType.Name with
          Fields = Model.toFields modelType
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
        names |> IndexSchema.fieldIndexesFromNames typeof<'m>
      in new UniqueIndex(fieldIndexes)

    static member Of([<ParamArray>] lambdas: array<Expression<Func<'m, obj>>>) =
      let fieldIndexes =
        lambdas |> Array.map (fun l -> l :> Expression)
        |> IndexSchema.fieldIndexesFromLambdas typeof<'m>
      in new UniqueIndex(fieldIndexes)
