namespace TokiwaDb.Core

open System
open System.IO

module HashTableDetail =
  type Hash = int64

  type HashTableElement<'k, 'v> =
    | Busy of 'k * 'v * Hash
    | Empty
    | Removed

open HashTableDetail

/// Hash table implemented using open addressing.
type HashTable<'k, 'v when 'k: equality>
  ( _hash: 'k -> Hash
  , _array: IResizeArray<HashTableElement<'k, 'v>>
  ) =
  let mutable _capacity = _array.Length
  let mutable _countBusy = 0L

  let _loadFactor () =
    (float _countBusy) / (float _capacity)

  let _hash key: Hash * int64 =
    let hash        = _hash key
    in (hash, ((hash % _capacity) + _capacity) % _capacity)

  let rec _rehash () =
    let capacity'   = _capacity * 2L + 7L
    let elements    =
      _array
      |> Seq.choose (function | Busy (k, v, h) -> Some (k, v) | _ -> None)
      |> Seq.toArray
    let ()          = 
      _capacity <- capacity'
      _countBusy <- 0L
      _array.SetAll(fun array' ->
        let () = array'.Initialize(capacity', Empty)
        let () =
          for (k, v) in elements do
            _updateImpl array' k v
        in ()
        )
      assert (_array.Length = _capacity)
    in ()

  and _insertImpl (array': IResizeArray<_>) f key value =
    let (hash, i0) = _hash key
    let rec loop i =
      if i = _capacity then
        _rehash()
        _insertImpl array' f key value
      else
        let h = (i0 + i) % _capacity
        match array'.Get(h) with
        | Busy (key', value', hash') when hash = hash' && key = key' ->
          array'.Set(h, Busy (key, f value' value, hash))
        | Busy _ ->
          loop (i + 1L)
        | Empty | Removed ->
          array'.Set(h, Busy (key, value, hash))
          _countBusy <- _countBusy + 1L
          if _loadFactor () >= 0.8 then
            _rehash()
    in loop 0L

  and _updateImpl array' =
    _insertImpl array' (fun v v' -> v')

  let _insert =
    _insertImpl _array

  let _update =
    _updateImpl _array

  let _remove key =
    let (hash, i0) = _hash key
    let rec loop i =
      if i < _capacity then
        let h = (i0 + i) % _capacity
        match _array.Get(h) with
        | Busy (key', _, hash') when hash = hash' && key = key' ->
          _array.Set(h, Removed)
          _countBusy <- _countBusy - 1L
          true
        | Busy _
        | Removed ->
          loop (i + 1L)
        | Empty -> false
      else false
    in loop 0L

  let _tryFindImpl key =
    let (hash, i0) = _hash key
    let rec loop i =
      if i = _capacity
      then None
      else
        let h = (i0 + i) % _capacity
        match _array.Get(h) with
        | Busy (key', value', hash') when hash = hash' && key = key' ->
          Some (h, key', value')
        | Busy _
        | Removed -> loop (i + 1L)
        | Empty -> None
    in loop 0L

  let _tryFind key =
    _tryFindImpl(key)
    |> Option.map (fun (_, _, value) -> value)

  do
    if _capacity = 0L then
      _capacity <- 7L
      _array.Initialize(_capacity, Empty)
      assert (_array.Length = _capacity)
    else
      _countBusy <- 
        _array
        |> Seq.map (function Busy _ -> 1L | _ -> 0L)
        |> Seq.sum

  new (array: IResizeArray<HashTableElement<'k, 'v>>) =
    HashTable((fun x -> x.GetHashCode() |> int64), array)

  member this.Length = _countBusy

  member this.Insert(key: 'k, value: 'v, f: 'v -> 'v -> 'v) =
    _insert f key value

  member this.Update(key: 'k, value: 'v) =
    _update key value

  member this.Remove(key: 'k) =
    _remove key

  member this.TryFind(key: 'k) =
    _tryFind key

type HashTableElementSerializer<'k, 'v>
  ( _keySerializer: FixedLengthSerializer<'k>
  , _valueSerializer: FixedLengthSerializer<'v>
  ) =
  inherit 
    FixedLengthUnionSerializer<HashTableElement<'k, 'v>>
      ([|
        FixedLengthTupleSerializer<'k * 'v * Hash>(
          [| _keySerializer; _valueSerializer; Int64Serializer() |])
        Int64Serializer()
        Int64Serializer()
      |])
