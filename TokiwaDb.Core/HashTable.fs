namespace TokiwaDb.Core

open System
open System.IO

module HashTableDetail =
  type Hash = int64

  type HashTableElement<'k, 'v> =
    | Busy of 'k * 'v * Hash
    | Empty
    | Removed

  type HashTableElementCombinator<'v> =
    | NoCombine
    | CombineWith     of ('v -> 'v -> 'v)

open HashTableDetail

/// Hash table with open addressing.
type HashTableBase<'k, 'v when 'k: equality>
  ( _hash: 'k -> Hash
  , _array: IResizeArray<HashTableElement<'k, 'v>>
  , _combi: HashTableElementCombinator<'v>
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
            _insertImpl array' _combi k v
        in ()
        )
      assert (_array.Length = _capacity)
    in ()

  and _insertImpl (array': IResizeArray<_>) combi key value =
    let (hash, i0) = _hash key
    let rec loop i =
      if i = _capacity then
        _rehash ()
        _insertImpl array' combi key value
      else
        let h = (i0 + i) % _capacity
        match array'.Get(h) with
        | Busy (key', value', hash') when hash = hash' && key = key' ->
          match combi with
          | NoCombine ->
            loop (i + 1L)
          | CombineWith f ->
            array'.Set(h, Busy (key, f value' value, hash))
        | Busy _ ->
          loop (i + 1L)
        | Empty | Removed ->
          array'.Set(h, Busy (key, value, hash))
          _countBusy <- _countBusy + 1L
          if _loadFactor () >= 0.8 then
            _rehash ()
    in loop 0L

  let _insert =
    _insertImpl _array

  let _remove onlyFirst key =
    let (hash, i0) = _hash key
    let rec loop i =
      seq {
        if i < _capacity then
          let h = (i0 + i) % _capacity
          match _array.Get(h) with
          | Busy (key', value', hash') when hash = hash' && key = key' ->
            _array.Set(h, Removed)
            _countBusy <- _countBusy - 1L
            yield value'
            yield! loop (i + 1L)
          | Busy _
          | Removed ->
            yield! loop (i + 1L)
          | Empty -> ()
        }
    in loop 0L

  let _findAll key =
    let (hash, i0) = _hash key
    let rec loop i =
      seq {
        if i < _capacity then
          let h = (i0 + i) % _capacity
          match _array.Get(h) with
          | Busy (key', value', hash') when hash = hash' && key = key' ->
            yield value'
            yield! loop (i + 1L)
          | Busy _
          | Removed -> yield! loop (i + 1L)
          | Empty -> ()
      }
    in loop 0L

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

  member this.Length = _countBusy

  member this.Insert(key, value, combinator) =
    _insert combinator key value

  member this.Remove(key: 'k, onlyFirst: bool) =
    _remove onlyFirst key

  member this.FindAll(key: 'k) =
    _findAll key

type HashTable<'k, 'v when 'k: equality>
  ( _hash: 'k -> Hash
  , _array: IResizeArray<HashTableElement<'k, 'v>>
  ) =
  let _updater = CombineWith (fun _ -> id)

  let _base = HashTableBase<'k, 'v>(_hash, _array, _updater)

  member this.Length = _base.Length

  member this.Insert(key, value, f) =
    _base.Insert(key, value, CombineWith f)

  member this.Update(key, value) =
    _base.Insert(key, value, _updater)

  member this.Remove(key: 'k) =
    _base.Remove(key, onlyFirst = true) |> Seq.isEmpty |> not

  member this.TryFind(key: 'k) =
    _base.FindAll(key) |> Seq.tryHead

type MultiHashTable<'k, 'v when 'k: equality>
  ( _hash: 'k -> Hash
  , _array: IResizeArray<HashTableElement<'k, 'v>>
  ) =
  let _base = HashTableBase<'k, 'v>(_hash, _array, NoCombine)

  member this.Length = _base.Length

  member this.Insert(key, value) =
    _base.Insert(key, value, NoCombine)

  member this.RemoveAll(key: 'k) =
    _base.Remove(key, onlyFirst = false)

  member this.FindAll(key: 'k) =
    _base.FindAll(key)
