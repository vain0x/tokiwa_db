namespace TokiwaDb.Core

open System
open System.IO

module HashTableDetail =
  type Hash = int64

  type HashTableElement<'k, 'v> =
    | Busy of 'k * 'v * Hash
    | Empty
    | Removed

  [<RequireQualifiedAccess>]
  type HashTableElementKind =
    | Busy = 0
    | Empty = 1
    | Removed = 2

open HashTableDetail

/// Hash table implemented using open addressing.
type HashTable<'k, 'v when 'k: equality>
  ( _hash: 'k -> Hash
  , _array: IResizeArray<HashTableElement<'k, 'v>>
  ) =
  let mutable _capacity = _array.Length
  let mutable _countBusy = 0L

  do
    if _capacity = 0L then
      _capacity <- 7L
      _array.Initialize(_capacity, Empty)
      assert (_array.Length = _capacity)
    else
      _countBusy <- 
        _array |> IResizeArray.toSeq
        |> Seq.map (function Busy _ -> 1L | _ -> 0L)
        |> Seq.sum

  new (array: IResizeArray<HashTableElement<'k, 'v>>) =
    HashTable((fun x -> x.GetHashCode() |> int64), array)

  member this.Length = _countBusy

  member private this.LoadFactor =
    (float _countBusy) / (float _capacity)

  member private this.Hash(key: 'k): Hash * int64 =
    let hash        = _hash key
    in (hash, ((hash % _capacity) + _capacity) % _capacity)

  member private this.Rehash() =
    let capacity'   = _capacity * 2L + 7L
    let elements    =
      _array |> IResizeArray.toSeq
      |> Seq.choose (function | Busy (k, v, h) -> Some (k, v) | _ -> None)
      |> Seq.toArray
    // TODO: Create a temporary file and rebuild this hash table in it then swap by renaming.
    let ()          = 
      _capacity <- capacity'
      _countBusy <- 0L
      _array.Initialize(capacity', Empty)
      assert (_array.Length = _capacity)
    let ()          =
      for (k, v) in elements do
        this.Update(k, v)
    in ()

  member this.Insert(key: 'k, value: 'v, f: 'v -> 'v -> 'v) =
    let (hash, i0) = this.Hash(key)
    let rec loop i =
      if i = _capacity then
        this.Rehash()
        this.Insert(key, value, f)
      else
        let h = (i0 + i) % _capacity
        match _array.Get(h) with
        | Busy (key', value', hash') when hash = hash' && key = key' ->
          _array.Set(h, Busy (key, f value' value, hash))
        | Busy _ ->
          loop (i + 1L)
        | Empty | Removed ->
          _array.Set(h, Busy (key, value, hash))
          _countBusy <- _countBusy + 1L
          if this.LoadFactor >= 0.8 then this.Rehash()
    in loop 0L

  member this.Update(key: 'k, value: 'v) =
    this.Insert(key, value, fun v v' -> v')

  member this.Remove(key: 'k) =
    let (hash, i0) = this.Hash(key)
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

  member private this.TryFindImpl(key: 'k) =
    let (hash, i0) = this.Hash(key)
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

  member this.TryFind(key: 'k) =
    this.TryFindImpl(key) |> Option.map (fun (_, _, value) -> value)

type HashTableElementSerializer<'k, 'v>(_keySerializer: FixedLengthSerializer<'k>, _valueSerializer: FixedLengthSerializer<'v>) =
  inherit FixedLengthSerializer<HashTableElement<'k, 'v>>()

  override this.Length =
    1L + 8L + _keySerializer.Length + _valueSerializer.Length

  override this.Serialize(element) =
    match element with
    | Busy (key, value, hash) ->
      [|
        yield HashTableElementKind.Busy |> byte
        yield! BitConverter.GetBytes(hash)
        yield! _keySerializer.Serialize(key)
        yield! _valueSerializer.Serialize(value)
      |]
    | Empty ->
      let xs      = Array.zeroCreate (this.Length |> int)
      let ()      = xs.[0] <- HashTableElementKind.Empty |> byte
      in xs
    | Removed ->
      let xs      = Array.zeroCreate (this.Length |> int)
      let ()      = xs.[0] <- HashTableElementKind.Removed |> byte
      in xs

  override this.Deserialize(bs) =
    match enum<HashTableElementKind>(bs.[0] |> int) with
    | HashTableElementKind.Busy ->
      let hash    = BitConverter.ToInt64(bs, 1)
      let key     = _keySerializer.Deserialize(bs.[9..(9 + int _keySerializer.Length - 1)])
      let i       = this.Length - _valueSerializer.Length |> int
      let value   = _valueSerializer.Deserialize(bs.[i..])
      in Busy (key, value, hash)
    | HashTableElementKind.Empty -> Empty
    | HashTableElementKind.Removed -> Removed
    | _ -> raise (Exception())
