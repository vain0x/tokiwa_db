using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// ref: http://qiita.com/ue_dai/items/0c0ec59fe9f747c73e35

namespace TokiwaDb.Core.Utilities
{
    public class PersistentBTree<Key, Value>
        where Key: IComparable
    {
        #region (Constants and Types)
        public const int _order = 128;

        public abstract class Node
        {
            /// <summary>
            /// Represents edges from internal nodes to this node.
            /// </summary>
            public List<InternalNode> Parents { get; private set; }

            public abstract IEnumerable<MortalElement> Elements { get; }

            public Node()
            {
                Parents = new List<InternalNode>();
            }

            public bool IsRootAt(long t)
            {
                return ParentAt(t) == null;
            }

            public InternalNode ParentAt(long t)
            {
                return Parents.Find(n => n.IsAliveAt(t));
            }

            /// <summary>
            /// Returns neighboring siblings to this node; or null.
            /// </summary>
            /// <param name="t"></param>
            /// <returns></returns>
            public Node[] NeighboringSiblingAt(long t)
            {
                var parent = ParentAt(t);
                if (parent == null) { return null; }
                return parent.NeighboringChildrenTo(t, this);
            }

            public bool IsAliveAt(long t)
            {
                return Elements.Any(e => e.IsAliveAt(t));
            }

            public bool IsFreshAt(long t)
            {
                return Elements.All(e => e.L == t);
            }

            public bool IsOverflowing
            {
                get { return Elements.Count() > _order; }
            }

            public bool IsUnderflowing
            {
                get { return Elements.Count() < _order * 3 / 4; }
            }

            public abstract void RemoveElementsAt(IEnumerable<int> indexes);
        }

        public class InternalNode
            : Node
        {
            private List<Tuple<Node, MortalElement>> _children;

            public override IEnumerable<MortalElement> Elements
            {
                get { return _children.Select(x => x.Item2); }
            }

            public InternalNode()
            {
                _children = new List<Tuple<Node, MortalElement>>();
            }

            /// <summary>
            /// Returns the element on the edge from this to `n`.
            /// Or null.
            /// </summary>
            /// <param name="n"></param>
            /// <returns></returns>
            public MortalElement FindElementTo(Node n)
            {
                return _children.Find(x => x.Item1 == n).Item2;
            }

            public void RemoveEdgeTo(Node n)
            {
                _children.Remove(_children.Find(x => x.Item1 == n));
            }

            public Node[] NeighboringChildrenTo(long t, Node n)
            {
                var children = _children.Where(c => c.Item2.IsAliveAt(t)).ToArray();
                for (var i = 0; i < children.Length; i++)
                {
                    var c = children[i];
                    if (c.Item1 == n)
                    {
                        var siblings = new List<Node>();
                        if (i > 0) { siblings.Add(children[i - 1].Item1); }
                        if (i + 1 < children.Length) { siblings.Add(children[i + 1].Item1); }
                        return siblings.ToArray();
                    }
                }
                return null;
            }

            public void Add(Node u, MortalElement e)
            {
                _children.Add(Tuple.Create(u, e));
            }

            public override void RemoveElementsAt(IEnumerable<int> indexes)
            {
                foreach (var i in indexes.OrderByDescending(i => i))
                {
                    _children.RemoveAt(i);
                }
            }
        }

        public class ExternalNode
            : Node
        {
            private List<MortalElement> _elements;

            public override IEnumerable<MortalElement> Elements
            {
                get { return _elements; }
            }

            public ExternalNode()
            {
                _elements = new List<MortalElement>();
            }

            public void Add(MortalElement e)
            {
                _elements.Add(e);
            }

            public void AddRange(IEnumerable<MortalElement> es)
            {
                _elements.AddRange(es);
            }

            public override void RemoveElementsAt(IEnumerable<int> indexes)
            {
                foreach (var i in indexes.OrderByDescending(i => i))
                {
                    _elements.RemoveAt(i);
                }
            }

            public List<MortalElement> DropAll()
            {
                var es = _elements;
                _elements = new List<MortalElement>();
                return es;
            }

            public List<MortalElement> DropLatterHalf()
            {
                var orderedElements =
                    _elements
                    .Select((x, i) => Tuple.Create(x, i))
                    .OrderBy(x => x.Item1.Key);

                var removedElements = new List<MortalElement>();
                var removedIndexes = new List<int>();
                for (var i = _elements.Count / 2; i < _elements.Count; i++)
                {
                    var kv = orderedElements.ElementAt(i);
                    removedElements.Add(kv.Item1);
                    removedIndexes.Add(kv.Item2);
                }

                RemoveElementsAt(removedIndexes);
                return removedElements;
            }
        }

        /// <summary>
        /// Element-internval pair.
        /// </summary>
        public class MortalElement
            : IComparable<MortalElement>
        {
            public KeyValuePair<Key, Value> KeyValuePair { get; private set; }
            public long L { get; private set; }
            public long R { get; private set; }
            
            public MortalElement(KeyValuePair<Key, Value> kv, long t)
            {
                KeyValuePair = kv;
                L = t;
                R = long.MaxValue;
            }

            public Key Key
            {
                get { return KeyValuePair.Key; }
            }

            public Value Value
            {
                get { return KeyValuePair.Value; }
            }

            public bool IsAliveAt(long t)
            {
                return L <= t && t < R;
            }

            // TODO: More accurate name.
            public bool IsImmortal
            {
                get { return R == long.MaxValue; }
            }

            public override int CompareTo(object source)
            {
                var rhs = (MortalElement)source;
                return Key.CompareTo(rhs.Key);
            }

            /// <summary>
            /// Perform logical deletion.
            /// </summary>
            /// <param name="t"></param>
            public void Kill(long t)
            {
                Debug.Assert(IsAliveAt(t));
                R = t;
            }
        }
        #endregion
        
        public long T { get; private set; }

        /// <summary>
        /// _roots[t] is the root node of T(t).
        /// </summary>
        public List<Node> _roots;

        public PersistentBTree()
        {
            T = 0;
            _roots = new List<Node>() { null };
        }

        public void SetRoot(long t, Node root)
        {
            Debug.Assert(_roots.Count >= t - 1);
            if (_roots.Count == t - 1)
            {
                _roots.Add(root);
            }
            else
            {
                _roots[(int)t] = root;
            }
        }

        public ExternalNode VersionCopy(Node u)
        {
            Debug.Assert(T > 0);

            var v = new ExternalNode();

            var removedElementIndexes = new List<int>();
            for (var i = 0; i < u.Elements.Count(); i++)
            {
                var e = u.Elements.ElementAt(i);
                if (!e.IsImmortal) continue;
                if (e.L < T)
                {
                    e.Kill(T);
                }
                else
                {
                    removedElementIndexes.Add(i);
                }

                v.Add(new MortalElement(e.KeyValuePair, T));
            }
            u.RemoveElementsAt(removedElementIndexes);

            var parent = u.ParentAt(T - 1);
            if (parent == null)
            {
                SetRoot(T, v);
            }
            else
            {
                foreach (var e in parent.Elements)
                {
                    if (!e.IsImmortal) continue;
                    e.Kill(T);
                }

                {
                    var e = parent.FindElementTo(u);
                    Debug.Assert(e != null);
                    parent.Add(v, new MortalElement(e.KeyValuePair, T));
                }
            }
            return v;
        }

        /// <summary>
        /// Split u into two nodes.
        /// Returns the new node and the parent of `u` after this operation.
        /// </summary>
        /// <param name="u"></param>
        /// <returns></returns>
        public Tuple<ExternalNode, InternalNode> Split(ExternalNode u)
        {
            {
                var countElements = u.Elements.Count();
                Debug.Assert(u.IsFreshAt(T));
                Debug.Assert((_order * 3 / 4) <= countElements && countElements <= (_order * 11 / 8));
            }

            var w = new ExternalNode();
            w.AddRange(u.DropLatterHalf());
            var parent = u.ParentAt(T);
            if (parent == null)
            {
                var newParent = new InternalNode();
                foreach (var v in new ExternalNode[] {u, w})
                {
                    var minElement = v.Elements.Min();
                    newParent.Add(v, new MortalElement(minElement.KeyValuePair, T));
                }
                return Tuple.Create(w, newParent);
            }
            else
            {
                var minElement = w.Elements.Min();
                parent.Add(w, new MortalElement(minElement.KeyValuePair, T));
                return Tuple.Create(w, parent);
            }
        }

        public void Merge(ExternalNode u0, ExternalNode u1)
        {
            u0.AddRange(u1.DropAll());

            var parent = u1.ParentAt(T);
            if (parent != null)
            {
                parent.RemoveEdgeTo(u1);

                if (parent == _roots[(int)T] && parent.Elements.Count(x => x.IsAliveAt(T)) == 1)
                {
                    SetRoot(T, u0);
                }
            }
            if (u0.Elements.Count() >= _order * 3/4)
            {
                Split(u0);
            }
        }

        private void RemedyOverflow(Node u)
        {
            var v = VersionCopy(u);
            if (v.Elements.Count() < _order * 3/8)
            {
                var siblings = u.NeighboringSiblingAt(T);
                if (siblings != null && siblings.Length > 0 && siblings[0] is ExternalNode)
                {
                    var w = (ExternalNode)siblings[0];
                    var x = VersionCopy(w);
                    Merge(w, v);
                }
            }
            else if (v.Elements.Count() > _order * 3/4)
            {
                Split(v);
            }

            var parent = u.ParentAt(T);
            if (parent != null && parent.IsOverflowing)
            {
                RemedyOverflow(parent);
            }
        }

        private void RemedyUnderflow(Node u)
        {

        }

        /// <summary>
        /// B木 T(t) を T(t - 1) の単なる複製として生成します。
        /// 直後に Insert か Delete の最初に使用します。
        /// </summary>
        private void DummyUpdate()
        {
            Debug.Assert(_roots.Count == T);
            _roots.Add(_roots[(int)T - 1]);
            T++;
        }

        public void Insert(KeyValuePair<Key, Value> kv)
        {

        }
    }
}
