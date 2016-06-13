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
    | Float         of double
    | String        of string
    | Time          of DateTime

  type ValuePointer =
    | PInt           of int64
    | PFloat         of double
    | PString        of pointer
    | PTime          of DateTime

  type ValueType =
    | TInt
    | TFloat
    | TString
    | TTime

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
  type Storage() =
    abstract member Derefer: ValuePointer -> Value
    abstract member Store: Value -> ValuePointer

  type IRelation =
    abstract member Name: string
    abstract member Type: array<Field>

    abstract member FindById: Id -> option<Record>
    abstract member RecordPointers: seq<RecordPointer>

    abstract member JoinOn: IRelation * array<Field> * array<Field> -> IRelation
    abstract member Projection: array<Field> -> IRelation
    abstract member Extend: (RecordPointer -> array<Field * Value>) -> IRelation

  type ITable =
    abstract member Schema: Schema
    abstract member Relation: IRelation

  type IDatabase =
    abstract member Name: string
    abstract member Tables: ITable
