namespace TokiwaDb.Core

type Relation(_fields: array<Field>, _recordPointers: seq<RecordPointer>) =
  interface IRelation with
    override this.Fields = _fields
    override this.RecordPointers = _recordPointers

    override this.Filter(pred) =
      Relation(_fields, _recordPointers |> Seq.filter pred) :> IRelation

    override this.Projection(names: Set<Name>) =
      let droppedIndexes =
        _fields
        |> Array.choosei (fun i (Field (name, _)) -> if names |> Set.contains name then Some i else None)
        |> Set.ofArray
      let projection () =
        Array.choosei (fun i x -> if droppedIndexes |> Set.contains i then None else Some x)
      in
        Relation
          ( _fields |> projection ()
          , _recordPointers |> Seq.map (projection ())
          ) :> IRelation

    override this.Rename(nameMap: Map<Name, Name>) =
      let fields =
        _fields |> Array.map (fun (Field (name, type') as field) ->
          match nameMap |> Map.tryFind name with
          | Some name' -> Field (name, type')
          | None -> field
          )
      in Relation(fields, _recordPointers) :> IRelation

    override this.Extend(extendedType, f) =
      Relation(extendedType, _recordPointers |> Seq.map f) :> IRelation

    override this.JoinOn(lFields, rFields, rRelation) =
      let lDict = Dicitonary<array<Value>, Set<array<Value>>>
      let rDict = Dictionary<array<Value>, 
