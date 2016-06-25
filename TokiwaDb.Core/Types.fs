namespace TokiwaDb.Core

open System
open System.Collections
open System.Collections.Generic
open Chessie.ErrorHandling

[<AutoOpen>]
module Types =
  /// A pointer to a storage.
  type StoragePointer = int64

  type RecordId = int64
  type TableId = int64

  /// A version number of database.
  /// The initial value is 0.
  type RevisionId = int64

  type [<AbstractClass>] RevisionServer() =
    abstract member Current: RevisionId
    abstract member Next: RevisionId
    abstract member Increase: unit -> RevisionId

  type IMortal =
    abstract member Birth: RevisionId
    abstract member Death: RevisionId

  type Mortal<'x> =
    {
      Birth: RevisionId
      Death: RevisionId
      Value: 'x
    }
  with
    interface IMortal with
      override this.Birth = this.Birth
      override this.Death = this.Death

  type Name = string

  type Value =
    | Int           of int64
    | Float         of float
    | String        of string
    | Time          of DateTime

  type ValuePointer =
    | PInt           of int64
    | PFloat         of float
    | PString        of StoragePointer
    | PTime          of DateTime

  type ValueType =
    | TInt
    | TFloat
    | TString
    | TTime

  type Field =
    | Field         of Name * ValueType

  type IndexSchema =
    | HashTableIndexSchema      of array<int>

  type TableSchema =
    {
      Name          : Name
      /// Except for id field.
      Fields        : array<Field>
      Indexes       : array<IndexSchema>
      LifeSpan      : Mortal<unit>
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

  [<RequireQualifiedAccess>]
  type Error =
    | TableAlreadyDroped  of TableId
    | WrongRecordType     of array<Field> * Record
    | DuplicatedRecord    of RecordPointer
    | InvalidRecordId     of RecordId

  type Operation =
    | InsertRecords       of TableId * array<RecordPointer>
    | RemoveRecords       of TableId * array<RecordId>

  type [<AbstractClass>] Transaction() =
    abstract member BeginCount: int
    abstract member Operations: seq<Operation>
    abstract member RevisionServer: RevisionServer

    abstract member Begin: unit -> unit
    abstract member Add: Operation -> unit
    abstract member Commit: unit -> unit
    abstract member Rollback: unit -> unit

    abstract member SyncRoot: obj

  type [<AbstractClass>] HashTableIndex() =
    abstract member Projection: RecordPointer -> RecordPointer
    abstract member TryFind: RecordPointer -> option<RecordId>

    abstract member Insert: RecordPointer * RecordId -> unit
    abstract member Remove: RecordPointer -> bool

  type [<AbstractClass>] Table() =
    abstract member Id: TableId
    abstract member Schema: TableSchema
    abstract member Relation: RevisionId -> Relation
    abstract member Database: Database
    abstract member Indexes: array<HashTableIndex>

    abstract member RecordById: RecordId -> option<Mortal<RecordPointer>>
    abstract member RecordPointers: seq<Mortal<RecordPointer>>

    abstract member PerformInsert: array<RecordPointer> -> unit
    abstract member PerformRemove: array<RecordId> -> unit
    abstract member Insert: array<Record> -> Result<array<RecordId>, Error>
    abstract member Remove: array<RecordId> -> Result<unit, Error>
    abstract member Drop: unit -> unit

    member this.Name = this.Schema.Name

    interface IMortal with
      override this.Birth = this.Schema.LifeSpan.Birth
      override this.Death = this.Schema.LifeSpan.Death

  and [<AbstractClass>] Database() =
    abstract member Name: string

    abstract member Transaction: Transaction
    abstract member Storage: Storage

    abstract member Tables: seq<Table>

    abstract member CreateTable: TableSchema -> Table
    abstract member Perform: array<Operation> -> unit

    member this.CurrentRevisionId = this.Transaction.RevisionServer.Current
