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

  /// A storage which stores values with size more than 64bit (e.g. string, blob).
  /// Data in storage must be immutable and unique.
  type [<AbstractClass>] Storage() =
    abstract member Derefer: ValuePointer -> Value
    abstract member Store: Value -> ValuePointer

  type [<AbstractClass>] Relation() =
    abstract member Fields: array<Field>
    abstract member RecordPointers: seq<RecordPointer>

    abstract member Filter: (RecordPointer -> bool) -> Relation
    abstract member Projection: array<Name> -> Relation
    abstract member Rename: Map<Name, Name> -> Relation
    abstract member Extend: array<Field> * (RecordPointer -> RecordPointer) -> Relation
    abstract member JoinOn: array<Name> * array<Name> * Relation -> Relation

    abstract member ToTuple: unit -> array<Field> * array<RecordPointer>

  type Mortal<'x> =
    {
      Begin: RevisionId
      End: RevisionId
      Value: 'x
    }
    
  type [<AbstractClass>] RevisionServer() =
    abstract member Current: RevisionId
    abstract member Next: unit -> RevisionId
    
  type [<AbstractClass>] Table() =
    abstract member Name: Name
    abstract member Schema: Schema
    abstract member Relation: RevisionId -> Relation
    abstract member Database: Database

    abstract member Insert: Record -> unit
    abstract member Delete: (RecordPointer -> bool) -> unit
    abstract member Delete: (Record -> bool) -> unit

  and [<AbstractClass>] Database() =
    abstract member SyncRoot: obj

    abstract member Name: string
    abstract member RevisionServer: RevisionServer
    abstract member Storage: Storage
    abstract member Tables: RevisionId -> seq<Table>

    abstract member CreateTable: Name * Schema -> Table
    abstract member DropTable: Name -> bool
