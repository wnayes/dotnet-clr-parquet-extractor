using Microsoft.Diagnostics.Runtime;
using System.Runtime.CompilerServices;
using System.Collections.Concurrent;

public class OptimizedDominatorAnalyzer
{
    private readonly ClrHeap _heap;

    // Mapping between real addresses and compressed indices
    private Dictionary<ulong, int>? _addressToIndex;
    private ulong[]? _indexToAddress;
    private ulong[]? _objectSizes;

    // Graph structure
    private List<int>[]? _successors;  // Forward edges: obj -> objects it references
    private List<int>[]? _predecessors; // Reverse edges: obj -> objects that reference it

    // Dominator computation results
    private int[]? _idom;              // Immediate dominator (in original node space)
    private ulong[]? _dominatedSizes;  // Total dominated size
    private int[]? _dominatedCounts;   // Count of dominated objects

    // DFS and LT algorithm working data
    private int[]? _dfnum;             // DFS number for each node (-1 if unreachable)
    private int[]? _vertex;            // Node at each DFS position
    private int[]? _parent;            // Parent in DFS tree (in DFS number space)
    private int[]? _semi;              // Semidominator (in DFS number space)
    private int[]? _ancestor;          // For disjoint set forest (in DFS number space)
    private int[]? _label;             // For path compression (in DFS number space)
    private List<int>[]? _bucket;      // Deferred processing (in DFS number space)

    private int _nodeCount;
    private int _dfsCount;
    private List<int>? _rootIndices;

    public OptimizedDominatorAnalyzer(ClrHeap heap)
    {
        _heap = heap;
    }

    public void ComputeDominators(IProgress<string> progress)
    {
        progress.Report("Phase 1: Enumerating objects...");
        EnumerateAndCompressObjects();

        progress.Report($"Phase 2: Building reference graph ({_nodeCount:N0} objects)...");
        BuildReferenceGraph();

        progress.Report("Phase 3: Running DFS from roots...");
        RunDFS();

        progress.Report("Phase 4: Computing dominators (Lengauer-Tarjan)...");
        ComputeDominatorsLT();

        progress.Report("Phase 5: Calculating dominated sizes...");
        CalculateDominatedSizes();

        progress.Report("Complete!");
    }

    private void EnumerateAndCompressObjects()
    {
        var tempObjects = new List<(ulong address, ulong size)>();

        foreach (var obj in _heap.EnumerateObjects())
        {
            var type = obj.Type;
            if (type == null) continue;

            var size = obj.Size;
            tempObjects.Add((obj.Address, size));
        }

        _nodeCount = tempObjects.Count;

        // Allocate arrays
        _addressToIndex = new Dictionary<ulong, int>(_nodeCount);
        _indexToAddress = new ulong[_nodeCount];
        _objectSizes = new ulong[_nodeCount];
        _successors = new List<int>[_nodeCount];
        _predecessors = new List<int>[_nodeCount];
        _idom = new int[_nodeCount];
        _dominatedSizes = new ulong[_nodeCount];
        _dominatedCounts = new int[_nodeCount];
        _dfnum = new int[_nodeCount];

        for (int i = 0; i < _nodeCount; i++)
        {
            var (address, size) = tempObjects[i];
            _addressToIndex[address] = i;
            _indexToAddress[i] = address;
            _objectSizes[i] = size;
            _successors[i] = new List<int>();
            _predecessors[i] = new List<int>();
            _idom[i] = -1;
            _dfnum[i] = -1; // -1 means unreachable
        }

        // Collect roots
        _rootIndices = new List<int>();
        var rootAddresses = new HashSet<ulong>();

        foreach (var root in _heap.EnumerateRoots())
        {
            if (root.Object != 0)
            {
                rootAddresses.Add(root.Object);
            }
        }

        foreach (var addr in rootAddresses)
        {
            if (_addressToIndex.TryGetValue(addr, out int idx))
            {
                _rootIndices.Add(idx);
            }
        }
    }

    private void BuildReferenceGraph()
    {
        if (_addressToIndex == null || _indexToAddress == null || _successors == null || _predecessors == null)
            throw new InvalidOperationException("Object enumeration must be done before building the reference graph.");

        int chunkSize = 10000;
        var chunks = Enumerable.Range(0, (_nodeCount + chunkSize - 1) / chunkSize);

        var referencesPerChunk = new ConcurrentBag<(int from, List<int> targets)>();

        Parallel.ForEach(chunks, chunkIndex =>
        {
            int start = chunkIndex * chunkSize;
            int end = Math.Min(start + chunkSize, _nodeCount);

            for (int i = start; i < end; i++)
            {
                var address = _indexToAddress[i];
                var obj = _heap.GetObject(address);
                if (!obj.IsValid) continue;

                var targets = new List<int>();

                foreach (var reference in obj.EnumerateReferences())
                {
                    if (reference != 0 && _addressToIndex.TryGetValue(reference, out int targetIdx))
                    {
                        targets.Add(targetIdx);
                    }
                }

                if (targets.Count > 0)
                {
                    referencesPerChunk.Add((i, targets));
                }
            }
        });

        // Build both forward and reverse graphs
        foreach (var (from, targets) in referencesPerChunk)
        {
            _successors[from].AddRange(targets);
            foreach (var to in targets)
            {
                _predecessors[to].Add(from);
            }
        }
    }

    private void RunDFS()
    {
        if (_dfnum == null || _successors == null || _rootIndices == null)
            throw new InvalidOperationException("Object enumeration must be done before running DFS.");

        _dfsCount = 0;

        _vertex = new int[_nodeCount];
        _parent = new int[_nodeCount];   // allocate NOW
        Array.Fill(_parent, -1);

        foreach (var rootIdx in _rootIndices)
        {
            if (_dfnum[rootIdx] == -1)
            {
                DFSVisit(rootIdx);
            }
        }

        Array.Resize(ref _vertex, _dfsCount);
        Array.Resize(ref _parent, _dfsCount);

        _semi = new int[_dfsCount];
        _ancestor = new int[_dfsCount];
        _label = new int[_dfsCount];
        _bucket = new List<int>[_dfsCount];

        for (int i = 0; i < _dfsCount; i++)
        {
            _semi[i] = i;
            _ancestor[i] = -1;
            _label[i] = i;
            _bucket[i] = new List<int>();
        }
    }

    private void DFSVisit(int v)
    {
        int vDfs = _dfsCount;
        _dfnum![v] = vDfs;
        _vertex![vDfs] = v;
        _dfsCount++;

        foreach (int w in _successors![v])
        {
            if (_dfnum[w] == -1)
            {
                DFSVisit(w);
                _parent![_dfnum[w]] = vDfs;
            }
        }
    }

    private void ComputeDominatorsLT()
    {
        // Lengauer-Tarjan algorithm
        // Process in reverse DFS order (skip 0 which is root)
        for (int i = _dfsCount - 1; i > 0; i--)
        {
            int w = i; // DFS number
            int nodeW = _vertex![w]; // Actual node

            // Step 2: Compute semidominator
            foreach (int predNode in _predecessors![nodeW])
            {
                int v = _dfnum![predNode]; // DFS number of predecessor
                if (v == -1) continue; // Unreachable predecessor

                int u = Eval(v);
                if (_semi![u] < _semi[w])
                {
                    _semi[w] = _semi[u];
                }
            }

            // Add w to bucket of its semidominator
            _bucket![_semi![w]].Add(w);

            // Link w to its parent
            int p = _parent![w];
            if (p >= 0)
            {
                Link(p, w);

                // Step 3: Process bucket of parent
                foreach (int v in _bucket[p])
                {
                    int u = Eval(v);
                    if (_semi[u] < _semi[v])
                    {
                        _idom![_vertex[v]] = _vertex[u]; // Store in node space
                    }
                    else
                    {
                        _idom![_vertex[v]] = _vertex[p]; // Store in node space
                    }
                }
                _bucket[p].Clear();
            }
        }

        // Step 4: Adjust immediate dominators
        for (int i = 1; i < _dfsCount; i++)
        {
            int w = i;
            int nodeW = _vertex![w];

            if (_idom![nodeW] != -1 && _idom[nodeW] != _vertex[_semi![w]])
            {
                // Follow idom chain
                _idom![nodeW] = _idom![_idom![nodeW]];
            }
        }

        // Roots have no dominator
        foreach (var rootIdx in _rootIndices!)
        {
            if (_dfnum![rootIdx] != -1)
            {
                _idom![rootIdx] = -1;
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Eval(int v)
    {
        if (_ancestor![v] == -1)
        {
            return v;
        }

        Compress(v);
        return _label![v];
    }

    private void Compress(int v)
    {
        var stack = new Stack<int>();
        int current = v;

        // Build path to root
        while (_ancestor![current] != -1 && _ancestor[_ancestor[current]] != -1)
        {
            stack.Push(current);
            current = _ancestor[current];
        }

        // Path compression
        while (stack.Count > 0)
        {
            current = stack.Pop();
            int anc = _ancestor[current];

            if (_semi![_label![anc]] < _semi[_label[current]])
            {
                _label[current] = _label[anc];
            }
            _ancestor[current] = _ancestor[anc];
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Link(int v, int w)
    {
        _ancestor![w] = v;
    }

    private void CalculateDominatedSizes()
    {
        // Build dominator tree
        var children = new List<int>[_nodeCount];
        for (int i = 0; i < _nodeCount; i++)
        {
            children[i] = new List<int>();
        }

        for (int i = 0; i < _nodeCount; i++)
        {
            if (_dfnum![i] == -1) continue; // Skip unreachable

            if (_idom![i] >= 0 && _idom![i] < _nodeCount)
            {
                children[_idom![i]].Add(i);
            }
        }

        // Calculate dominated sizes bottom-up
        var visited = new HashSet<int>();

        foreach (var rootIdx in _rootIndices!)
        {
            if (_dfnum![rootIdx] != -1 && !visited.Contains(rootIdx))
            {
                CalculateDominatedSizeRecursive(rootIdx, children, visited);
            }
        }
    }

    private void CalculateDominatedSizeRecursive(int node, List<int>[] children, HashSet<int> visited)
    {
        var stack = new Stack<(int node, bool processed)>();
        stack.Push((node, false));

        while (stack.Count > 0)
        {
            var (current, processed) = stack.Pop();

            if (processed)
            {
                // Post-order processing: calculate size after children
                _dominatedSizes![current] = _objectSizes![current];
                _dominatedCounts![current] = 1;

                foreach (int child in children[current])
                {
                    _dominatedSizes![current] += _dominatedSizes![child];
                    _dominatedCounts![current] += _dominatedCounts![child];
                }
                continue;
            }

            if (visited.Contains(current))
                continue;

            visited.Add(current);

            // Push for post-order processing
            stack.Push((current, true));

            // Push children
            foreach (int child in children[current])
            {
                if (!visited.Contains(child))
                {
                    stack.Push((child, false));
                }
            }
        }
    }

    public IEnumerable<DominatorInfo> GetTopDominators(int limit)
    {
        int count = limit;

        var reachableIndices = new List<int>();
        for (int i = 0; i < _nodeCount; i++)
        {
            if (_dfnum![i] != -1) // Reachable
            {
                reachableIndices.Add(i);
            }
        }

        var sortedIndices = reachableIndices
            .OrderByDescending(idx => _dominatedSizes![idx])
            .Take(count)
            .ToList();

        foreach (int idx in sortedIndices)
        {
            var address = _indexToAddress![idx];
            var type = _heap.GetObjectType(address);

            yield return new DominatorInfo
            {
                ObjectAddress = address,
                ImmediateDominator = _idom![idx] >= 0 ? _indexToAddress[_idom[idx]] : 0,
                DominatedSize = _dominatedSizes![idx],
                DominatedCount = _dominatedCounts![idx],
                TypeName = type?.Name ?? "Unknown",
                ObjectSize = _objectSizes![idx]
            };
        }
    }

    public DominatorData GetAllDominatorData()
    {
        var addresses = new List<ulong>();
        var immediateDoms = new List<ulong>();
        var domSizes = new List<ulong>();
        var domCounts = new List<int>();

        for (int i = 0; i < _nodeCount; i++)
        {
            if (_dfnum![i] != -1) // Reachable
            {
                addresses.Add(_indexToAddress![i]);
                immediateDoms.Add(_idom![i] >= 0 ? _indexToAddress[_idom![i]] : 0UL);
                domSizes.Add(_dominatedSizes![i]);
                domCounts.Add(_dominatedCounts![i]);
            }
        }

        return new DominatorData
        {
            ObjectAddresses = addresses.ToArray(),
            ImmediateDominators = immediateDoms.ToArray(),
            DominatedSizes = domSizes.ToArray(),
            DominatedCounts = domCounts.ToArray()
        };
    }

    public int GetReachableObjectCount() => _dfnum!.Count(x => x != -1);
    public int GetTotalObjectCount() => _nodeCount;
}

public class DominatorInfo
{
    public ulong ObjectAddress { get; set; }
    public ulong ImmediateDominator { get; set; }
    public ulong DominatedSize { get; set; }
    public int DominatedCount { get; set; }
    public string? TypeName { get; set; }
    public ulong ObjectSize { get; set; }
}

public class DominatorData
{
    public required ulong[] ObjectAddresses { get; set; }
    public required ulong[] ImmediateDominators { get; set; }
    public required ulong[] DominatedSizes { get; set; }
    public required int[] DominatedCounts { get; set; }
}

// Usage example:
// public class Program
// {
//     public static void Main()
//     {
//         using var dataTarget = DataTarget.LoadDump(@"C:\dumps\myapp.dmp");
//         var runtime = dataTarget.ClrVersions[0].CreateRuntime();
//         var heap = runtime.Heap;

//         var progress = new Progress<string>(msg => Console.WriteLine(msg));

//         // Only compute top 10,000 dominators to save memory
//         var analyzer = new OptimizedDominatorAnalyzer(heap, topNLimit: 10000);
//         analyzer.ComputeDominators(progress);

//         Console.WriteLine("\nTop 20 dominators:");
//         foreach (var info in analyzer.GetTopDominators(20))
//         {
//             Console.WriteLine($"{info.ObjectAddress:X16}: {info.TypeName}");
//             Console.WriteLine($"  Object Size: {info.ObjectSize:N0} bytes");
//             Console.WriteLine($"  Dominated Size: {info.DominatedSize:N0} bytes");
//             Console.WriteLine($"  Dominated Count: {info.DominatedCount:N0} objects");
//             Console.WriteLine($"  Retention Ratio: {(double)info.DominatedSize / info.ObjectSize:F1}x");
//             Console.WriteLine();
//         }
//     }
// }