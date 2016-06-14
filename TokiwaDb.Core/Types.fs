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
  type RevisionId = int64

  type Name = string

  type Value =
    | Int           of int64
    | Float         of float
    | String        of string
    | Time          of DateTime

  type ValuePointer =
    | PInt           of int64
    | PFloat         of float
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

  type IStorage =
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

  type Mortal<'x> =
    {
      Begin: RevisionId
      End: RevisionId
      Value: 'x
    }

  type IRevisionServer =
    abstract member Current: RevisionId
    abstract member Next: unit -> RevisionId

  type ITable =
    abstract member Name: Name
    abstract member Schema: Schema
    abstract member Relation: RevisionId -> IRelation

    abstract member Insert: IRevisionServer * RecordPointer -> unit
    abstract member Delete: IRevisionServer * (RecordPointer -> bool) -> unit

  type IDatabase =
    abstract member Name: string
    abstract member Tables: ITable
    abstract member RevisionServer: IRevisionServer
