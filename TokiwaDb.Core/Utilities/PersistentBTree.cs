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

            public bool IsAliveAt(long t)
            {
                return Elements.Any(e => e.IsAliveAt(t));
            }

            public bool IsFreshAt(long t)
            {
                return Elements.All(e => e.L == t);
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

            public override void RemoveElementsAt(IEnumerable<int> indexes)
            {
                foreach (var i in indexes.OrderByDescending(i => i))
                {
                    _elements.RemoveAt(i);
                }
            }
        }

        /// <summary>
        /// Element-internval pair.
        /// </summary>
        public class MortalElement
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

        public void Split(Node u)
        {
            {
                var countElements = u.Elements.Count();
                Debug.Assert(u.IsFreshAt(T));
                Debug.Assert((_order * 3 / 4) <= countElements && countElements <= (_order * 11 / 8));
            }
        }
    }
}
