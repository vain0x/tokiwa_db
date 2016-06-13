namespace TokiwaDb.Core

open System

[<AutoOpen>]
module Types =
  /// A pointer to a storage.
  type pointer = int64

  /// Identity number.
  type Id = int64

  /// A version number of database.
  /// The initial value is 0.
  type RevId = int64

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
      KeyFields     : KeyFields
      NonkeyFields  : array<Field>
    }

  type Record =
    array<Value>

  type RecordPointer =
    array<ValuePointer>

  [<AbstractClass>]
  type Storage() =
    abstract member Derefer: ValuePointer -> Value
    abstract member Store: Value -> ValuePointer

  type IRelation =
    abstract member Fields: array<Field>
    abstract member RecordPointers: seq<RecordPointer>

    abstract member Filter: (RecordPointer -> bool) -> IRelation
    abstract member Projection: array<Name> -> IRelation
    abstract member Rename: Map<Name, Name> -> IRelation
    abstract member Extend: array<Field> * (RecordPointer -> RecordPointer) -> IRelation
    abstract member JoinOn: array<Name> * array<Name> * IRelation -> IRelation

    abstract member ToTuple: unit -> array<Field> * array<RecordPointer>

  type ITable =
    abstract member Name: Name
    abstract member Schema: Schema
    abstract member Relation: IRelation

  type IDatabase =
    abstract member Name: string
    abstract member Tables: ITable
