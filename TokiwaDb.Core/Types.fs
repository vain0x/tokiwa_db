namespace TokiwaDb.Core

open System

[<AutoOpen>]
module Types =
  /// A pointer to a storage.
  type pointer = int64

  /// Identity number.
  type Id = int64

  type Name = string

  type Value =
    | Int           of int64
    | Double        of double
    | String        of string
    | Time          of DateTime

  type ValuePointer =
    | Int           of int64
    | Double        of double
    | String        of pointer
    | Time          of DateTime

  [<RequireQualifiedAccess>]
  type ValueType =
    | Int
    | Double
    | String
    | Time

  type Field =
    | Field         of Name * ValueType

  type KeyFields =
    /// Auto-increment unique integer.
    | Id
    | KeyFields     of array<Field>

  type Schema =
    {
      Name          : string
      KeyFields     : KeyFields
      NonkeyFields  : array<Field>
    }

  type Record =
    Map<Field, Value>

  type RecordPointer =
    Map<Field, ValuePointer>

  [<AbstractClass>]
  type Storage =
    abstract member Derefer: ValuePointer -> Value
    abstract member Store: Value -> ValuePointer

  type IRelation =
    abstract member Name: string
    abstract member Schema: array<Field>

    abstract member FindById: Id -> option<Record>
    abstract member RecordPointers: seq<RecordPointer>
    abstract member Records: IStorage -> seq<Record>

    abstract member JoinOn: IRelation * array<Field> * array<Field> -> IRelation
    abstract member Projection: array<Field> -> IRelation
    abstract member Extend: (RecordPointer -> array<Field * Value>) -> IRelation

  type ITable =
    abstract member Schema: Schema
    abstract member Relation: IRelation

  type IDatabase =
    abstract member Name: string
    abstract member TableSet: Map<Name, ITable>
