namespace TokiwaDb.Core

open System.Collections.Generic

type NaiveRelation(_fields: array<Field>, _recordPointers: seq<RecordPointer>) =
  inherit Relation()

  override this.Fields = _fields
  override this.RecordPointers = _recordPointers

  override this.ToTuple() =
    (_fields, _recordPointers |> Seq.toArray)

  override this.Filter(pred) =
    NaiveRelation(_fields, _recordPointers |> Seq.filter pred) :> Relation

  override this.Projection(names: array<Name>) =
    let projectionIndexes =
      let map =
        _fields |> Array.mapi (fun i (Field (name, _)) -> (name, i))
        |> Map.ofArray
      in
        names |> Array.choose (fun name -> map |> Map.tryFind name)
    let projection (xs: array<_>) =
      projectionIndexes |> Array.map (fun i -> xs.[i])
    in
      NaiveRelation
        ( _fields |> projection
        , _recordPointers |> Seq.map projection
        ) :> Relation

  override this.Rename(nameMap: Map<Name, Name>) =
    let fields =
      _fields |> Array.map (fun (Field (name, type') as field) ->
        match nameMap |> Map.tryFind name with
        | Some name' -> Field (name', type')
        | None -> field
        )
    in NaiveRelation(fields, _recordPointers) :> Relation

  override this.Extend(extendedType, f) =
    NaiveRelation(extendedType, _recordPointers |> Seq.map f) :> Relation

  override this.JoinOn(lFields, rFields, rRelation) =
    let partitionFields allFields names =
      let fieldIndexFromName =
        let map =
          allFields |> Array.mapi (fun i (Field (name, _)) -> (name, i)) |> Map.ofArray
        in fun name -> map |> Map.tryFind name
      let jointIndexes =
        names |> Array.choose fieldIndexFromName
      let restFields =
        allFields |> Array.mapi (fun i field ->
          if jointIndexes |> Array.exists ((=) i) then None else Some field
          )
        |> Array.choose id
      in (jointIndexes, restFields)
    /// Create a dictionary from records by dividing each of them into two parts.
    /// Keys are joint parts and their values are the rest parts.
    let toDict jointIndexes recordPointers =
      let dict = Dictionary<_, _>()
      for (recordPointer: array<_>) in recordPointers do
        let joint =
          jointIndexes
          |> Array.map (fun i -> recordPointer.[i])
          |> Array.toList  // For structural comparison.
        let rest =
          recordPointer |> Array.choosei (fun i x ->
            if jointIndexes |> Array.exists ((=) i)
            then None
            else Some x
            )
        match dict.TryGetValue(joint) with
        | (true, s) -> dict.[joint] <- s |> Set.add rest
        | (false, _) -> dict.Add(joint, Set.singleton rest) |> ignore
      dict
    let (lIndexes, lRestFields) = lFields |> partitionFields _fields
    let (rIndexes, rRestFields) = rFields |> partitionFields rRelation.Fields
    let lDict = _recordPointers |> toDict lIndexes
    let rDict = rRelation.RecordPointers |> toDict rIndexes
    let recordPointers =
      seq {
        for (KeyValue (joint, lHalves)) in lDict do
          match rDict.TryGetValue(joint) with
          | (true, rHalves) ->
            for lHalf in lHalves do
              for rHalf in rHalves ->
                [|
                    yield! lHalf
                    yield! joint
                    yield! rHalf
                |]
          | (false, _) -> ()
      }
    let fields =
      [|
        yield! lRestFields
        yield! (lIndexes |> Array.map (fun i -> _fields.[i]))
        yield! rRestFields
      |]
    in
      NaiveRelation(fields, recordPointers) :> Relation
