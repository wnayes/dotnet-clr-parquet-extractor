using Microsoft.Diagnostics.Runtime;
using Parquet;
using Parquet.Data;

if (args.Length == 0)
{
    Console.Error.WriteLine("Usage: DotnetCLRParquetExtractor <dumpPath>");
    return 1;
}

string dumpPath = args[0];
string outputDirectory = Path.Combine(Path.GetDirectoryName(dumpPath) ?? ".", "parquet");
Directory.CreateDirectory(outputDirectory);

string objectsDbPath = Path.Combine(outputDirectory, "objects_blob.sqlite");
ObjectsDb objectsDb = new(objectsDbPath);

string stringsDbPath = Path.Combine(outputDirectory, "strings.sqlite");
StringsDb stringsDb = new(stringsDbPath);

using DataTarget dataTarget = DataTarget.LoadDump(dumpPath);

await WriteModulesParquetAsync(outputDirectory, dataTarget.EnumerateModules().ToArray());

List<RuntimeInfo> runtimeInfos = [];
int clrIndex = 0;
foreach (ClrInfo runtime in dataTarget.ClrVersions)
{
    using var clr = runtime.CreateRuntime();

    int runtimeId = clrIndex++;

    runtimeInfos.Add(new RuntimeInfo
    {
        Index = runtimeId,
        Flavor = runtime.Flavor.ToString(),
        Version = runtime.Version?.ToString() ?? "unknown"
    });

    await WriteObjectDataParquetAsync(outputDirectory, clr, runtimeId, objectsDb, stringsDb, dataTarget);
    await WriteHandlesParquetAsync(outputDirectory, clr, runtimeId, dataTarget);
    await WriteRootsParquetAsync(outputDirectory, clr, runtimeId, dataTarget);
    await WriteDominatorsParquetAsync(outputDirectory, clr, runtimeId);
}

await objectsDb.CompleteAndCloseAsync();
await stringsDb.CompleteAndCloseAsync();

await WriteRuntimesParquetAsync(outputDirectory, runtimeInfos);

static async Task WriteObjectDataParquetAsync(
    string outputDirectory,
    ClrRuntime clrRuntime,
    int runtimeId,
    ObjectsDb objectsDb,
    StringsDb stringsDb,
    DataTarget dataTarget
)
{
    const int ListInitialCapacity = 8192;

    string typesOutPath = Path.Combine(outputDirectory, "types.parquet");
    Dictionary<ClrType, int> typeMap = new Dictionary<ClrType, int>(1024);
    var typeIds = new List<int>(ListInitialCapacity);
    var runtimeIds = new List<int>(ListInitialCapacity);
    var methodTables = new List<ulong>(ListInitialCapacity);
    var names = new List<string>(ListInitialCapacity);
    var isArray = new List<bool>(ListInitialCapacity);
    var isString = new List<bool>(ListInitialCapacity);
    var isValueType = new List<bool>(ListInitialCapacity);
    var staticSizes = new List<int>(ListInitialCapacity);

    string staticFieldsOutPath = Path.Combine(outputDirectory, "static_fields.parquet");
    var staticFieldRuntimeIds = new List<int>(ListInitialCapacity);
    var staticFieldTypeIds = new List<int>(ListInitialCapacity);
    var staticFieldNames = new List<string>(ListInitialCapacity);
    var staticFieldSizes = new List<int>(ListInitialCapacity);
    var staticFieldOffsets = new List<int>(ListInitialCapacity);
    var staticFieldToObjectIds = new List<ulong>(ListInitialCapacity);

    string objectsOutPath = Path.Combine(outputDirectory, "objects.parquet");
    var objIds = new List<ulong>(ListInitialCapacity);
    var objRuntimeIds = new List<int>(ListInitialCapacity);
    var objTypeIds = new List<int>(ListInitialCapacity);
    var objSizes = new List<ulong>(ListInitialCapacity);
    var objGenerations = new List<int>(ListInitialCapacity);

    string referencesOutPath = Path.Combine(outputDirectory, "references.parquet");
    List<ulong> refFrom = new(ListInitialCapacity);
    List<ulong> refTo = new(ListInitialCapacity);
    List<int> refRuntimeIds = new(ListInitialCapacity);
    List<string> refField = new(ListInitialCapacity);

    ClrHeap heap = clrRuntime.Heap;

    if (!heap.CanWalkHeap)
    {
        Console.Error.WriteLine($"Cannot walk heap for runtime {runtimeId}, skipping.");
        return;
    }

    foreach (ClrSegment seg in heap.Segments)
    {
        foreach (ClrObject obj in seg.EnumerateObjects())
        {
            ClrType? type = heap.GetObjectType(obj);
            if (type == null) continue;

            if (!typeMap.TryGetValue(type, out int typeId))
            {
                typeId = typeMap.Count;
                typeMap[type] = typeId;

                typeIds.Add(typeId);
                runtimeIds.Add(runtimeId);
                methodTables.Add(type.MethodTable);
                names.Add(type.Name ?? "<unknown>");
                isArray.Add(type.IsArray);
                isString.Add(type.IsString);
                isValueType.Add(type.IsValueType);
                staticSizes.Add(type.StaticSize);

                foreach (ClrStaticField staticField in type.StaticFields)
                {
                    staticFieldRuntimeIds.Add(runtimeId);
                    staticFieldTypeIds.Add(typeId);
                    staticFieldNames.Add(staticField.Name ?? "<unknown>");
                    staticFieldSizes.Add(staticField.Size);
                    staticFieldOffsets.Add(staticField.Offset);
                    if (staticField.IsObjectReference)
                    {
                        // Always returns zero? https://github.com/microsoft/clrmd/issues/1313
                        staticFieldToObjectIds.Add(
                            staticField.Read<ulong>(clrRuntime.AppDomains.First()));
                    }
                    else
                    {
                        staticFieldToObjectIds.Add(0);
                    }
                }
            }

            objIds.Add(obj.Address);
            objRuntimeIds.Add(runtimeId);
            objTypeIds.Add(typeMap[type]);
            objSizes.Add(obj.Size);
            objGenerations.Add((int)seg.GetGeneration(obj));

            await WriteObjectBlobToDatabaseAsync(objectsDb, obj, dataTarget);

            if (type.IsString)
            {
                await WriteStringToDatabaseAsync(stringsDb, obj);
            }

            foreach (ClrReference reference in obj.EnumerateReferencesWithFields(false, true))
            {
                ClrObject target = reference.Object;
                refFrom.Add(obj.Address);
                refTo.Add(target.Address);
                refRuntimeIds.Add(runtimeId);
                refField.Add(reference.IsField ? (reference.Field?.Name ?? "<unknown>") : "");
            }
        }
    }

    using (Stream typesFileStream = File.Create(typesOutPath))
    using (ParquetWriter typesWriter = await ParquetWriter.CreateAsync(Schemas.Types, typesFileStream, new ParquetOptions(), false))
    using (ParquetRowGroupWriter typesGroupWriter = typesWriter.CreateRowGroup())
    {
        typesWriter.CompressionMethod = CompressionMethod.Zstd;
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[0], typeIds.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[1], runtimeIds.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[2], methodTables.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[3], names.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[4], isArray.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[5], isString.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[6], isValueType.ToArray()));
        await typesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Types.DataFields[7], staticSizes.ToArray()));
    }

    using (Stream staticFieldsFileStream = File.Create(staticFieldsOutPath))
    using (ParquetWriter staticFieldsWriter = await ParquetWriter.CreateAsync(Schemas.StaticFields, staticFieldsFileStream, new ParquetOptions(), false))
    using (ParquetRowGroupWriter staticFieldsGroupWriter = staticFieldsWriter.CreateRowGroup())
    {
        staticFieldsWriter.CompressionMethod = CompressionMethod.Zstd;
        await staticFieldsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.StaticFields.DataFields[0], staticFieldRuntimeIds.ToArray()));
        await staticFieldsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.StaticFields.DataFields[1], staticFieldTypeIds.ToArray()));
        await staticFieldsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.StaticFields.DataFields[2], staticFieldNames.ToArray()));
        await staticFieldsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.StaticFields.DataFields[3], staticFieldSizes.ToArray()));
        await staticFieldsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.StaticFields.DataFields[4], staticFieldOffsets.ToArray()));
        await staticFieldsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.StaticFields.DataFields[5], staticFieldToObjectIds.ToArray()));
    }

    using (Stream objectsFileStream = File.Create(objectsOutPath))
    using (ParquetWriter objectsWriter = await ParquetWriter.CreateAsync(Schemas.Objects, objectsFileStream, new ParquetOptions(), false))
    using (ParquetRowGroupWriter objectsGroupWriter = objectsWriter.CreateRowGroup())
    {
        objectsWriter.CompressionMethod = CompressionMethod.Zstd;
        await objectsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Objects.DataFields[0], objIds.ToArray()));
        await objectsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Objects.DataFields[1], objRuntimeIds.ToArray()));
        await objectsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Objects.DataFields[2], objTypeIds.ToArray()));
        await objectsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Objects.DataFields[3], objSizes.ToArray()));
        await objectsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Objects.DataFields[4], objGenerations.ToArray()));
    }

    using (Stream referencesFileStream = File.Create(referencesOutPath))
    using (ParquetWriter referencesWriter = await ParquetWriter.CreateAsync(Schemas.References, referencesFileStream, new ParquetOptions(), false))
    using (ParquetRowGroupWriter referencesGroupWriter = referencesWriter.CreateRowGroup())
    {
        referencesWriter.CompressionMethod = CompressionMethod.Zstd;
        await referencesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.References.DataFields[0], refFrom.ToArray()));
        await referencesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.References.DataFields[1], refTo.ToArray()));
        await referencesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.References.DataFields[2], refRuntimeIds.ToArray()));
        await referencesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.References.DataFields[3], refField.ToArray()));
    }
}

static async Task WriteHandlesParquetAsync(
    string outputDirectory,
    ClrRuntime clrRuntime,
    int runtimeId,
    DataTarget dataTarget
)
{
    const int ListInitialCapacity = 8192;

    string objectsOutPath = Path.Combine(outputDirectory, "objects.parquet");
    var handleKinds = new List<byte>(ListInitialCapacity);
    var handleAddresses = new List<ulong>(ListInitialCapacity);
    var handleObjectAddresses = new List<ulong>(ListInitialCapacity);
    var handleDependentAddresses = new List<ulong>(ListInitialCapacity);
    var handleReferenceCounts = new List<uint>(ListInitialCapacity);
    var handleRootKinds = new List<byte>(ListInitialCapacity);
    var handleIsInteriors = new List<bool>(ListInitialCapacity);
    var handleRuntimeIds = new List<int>(ListInitialCapacity);

    foreach (var handle in clrRuntime.EnumerateHandles())
    {
        handleKinds.Add((byte)handle.HandleKind);
        handleAddresses.Add(handle.Address);
        handleObjectAddresses.Add(handle.Object.Address);
        handleDependentAddresses.Add(handle.Dependent.Address);
        handleReferenceCounts.Add(handle.ReferenceCount);
        handleRootKinds.Add((byte)handle.RootKind);
        handleIsInteriors.Add(handle.IsInterior);
        handleRuntimeIds.Add(runtimeId);
    }

    string handlesOutPath = Path.Combine(outputDirectory, "handles.parquet");
    using (Stream handlesFileStream = File.Create(handlesOutPath))
    using (ParquetWriter handlesWriter = await ParquetWriter.CreateAsync(Schemas.Handles, handlesFileStream, new ParquetOptions(), false))
    using (ParquetRowGroupWriter handlesGroupWriter = handlesWriter.CreateRowGroup())
    {
        handlesWriter.CompressionMethod = CompressionMethod.Zstd;
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[0], handleKinds.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[1], handleAddresses.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[2], handleObjectAddresses.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[3], handleDependentAddresses.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[4], handleReferenceCounts.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[5], handleRootKinds.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[6], handleIsInteriors.ToArray()));
        await handlesGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Handles.DataFields[7], handleRuntimeIds.ToArray()));
    }
}

static async Task WriteRootsParquetAsync(
    string outputDirectory,
    ClrRuntime clrRuntime,
    int runtimeId,
    DataTarget dataTarget
)
{
    const int ListInitialCapacity = 8192;

    string objectsOutPath = Path.Combine(outputDirectory, "objects.parquet");
    var handleAddresses = new List<ulong>(ListInitialCapacity);
    var handleObjectAddresses = new List<ulong>(ListInitialCapacity);
    var handleRootKinds = new List<byte>(ListInitialCapacity);
    var handleIsInteriors = new List<bool>(ListInitialCapacity);
    var handleRuntimeIds = new List<int>(ListInitialCapacity);

    foreach (ClrRoot root in clrRuntime.Heap.EnumerateRoots())
    {
        handleAddresses.Add(root.Address);
        handleObjectAddresses.Add(root.Object.Address);
        handleRootKinds.Add((byte)root.RootKind);
        handleIsInteriors.Add(root.IsInterior);
        handleRuntimeIds.Add(runtimeId);
    }

    string rootsOutPath = Path.Combine(outputDirectory, "roots.parquet");
    using (Stream rootsFileStream = File.Create(rootsOutPath))
    using (ParquetWriter rootsWriter = await ParquetWriter.CreateAsync(Schemas.Roots, rootsFileStream, new ParquetOptions(), false))
    using (ParquetRowGroupWriter rootsGroupWriter = rootsWriter.CreateRowGroup())
    {
        rootsWriter.CompressionMethod = CompressionMethod.Zstd;
        await rootsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Roots.DataFields[0], handleAddresses.ToArray()));
        await rootsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Roots.DataFields[1], handleObjectAddresses.ToArray()));
        await rootsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Roots.DataFields[2], handleRootKinds.ToArray()));
        await rootsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Roots.DataFields[3], handleIsInteriors.ToArray()));
        await rootsGroupWriter.WriteColumnAsync(new DataColumn(Schemas.Roots.DataFields[4], handleRuntimeIds.ToArray()));
    }
}

static async Task WriteModulesParquetAsync(string outputDirectory, ModuleInfo[] moduleInfos)
{
    string outPath = Path.Combine(outputDirectory, "modules.parquet");
    using Stream fileStream = File.Create(outPath);
    using ParquetWriter writer = await ParquetWriter.CreateAsync(Schemas.Modules, fileStream, new ParquetOptions(), false);
    using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();

    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[0], moduleInfos.Select(mi => mi.FileName).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[1], moduleInfos.Select(mi => mi.IsManaged).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[2], moduleInfos.Select(mi => mi.Kind.ToString()).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[3], moduleInfos.Select(mi => mi.ImageBase).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[4], moduleInfos.Select(mi => mi.ImageSize).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[5], moduleInfos.Select(mi => mi.IndexFileSize).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Modules.DataFields[6], moduleInfos.Select(mi => mi.Version?.ToString()).ToArray()));
}

static async Task WriteRuntimesParquetAsync(string outputDirectory, List<RuntimeInfo> runtimeInfos)
{
    string outPath = Path.Combine(outputDirectory, "runtimes.parquet");
    using Stream fileStream = File.Create(outPath);
    using ParquetWriter writer = await ParquetWriter.CreateAsync(Schemas.Runtimes, fileStream, new ParquetOptions(), false);
    using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();

    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Runtimes.DataFields[0], runtimeInfos.Select(ri => ri.Index).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Runtimes.DataFields[1], runtimeInfos.Select(ri => ri.Flavor).ToArray()));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Runtimes.DataFields[2], runtimeInfos.Select(ri => ri.Version).ToArray()));
}

static async Task WriteDominatorsParquetAsync(string outputDirectory, ClrRuntime runtime, int runtimeId)
{
    string outPath = Path.Combine(outputDirectory, "dominators.parquet");
    using Stream fileStream = File.Create(outPath);
    using ParquetWriter writer = await ParquetWriter.CreateAsync(Schemas.Dominators, fileStream, new ParquetOptions(), false);
    using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();

    var progress = new Progress<string>(msg => Console.WriteLine(msg));
    var analyzer = new OptimizedDominatorAnalyzer(runtime.Heap);
    analyzer.ComputeDominators(progress);
    var dominatorData = analyzer.GetAllDominatorData();

    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Dominators.DataFields[0], dominatorData.ObjectAddresses));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Dominators.DataFields[1], dominatorData.ImmediateDominators));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Dominators.DataFields[2], dominatorData.DominatedSizes));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Dominators.DataFields[3], dominatorData.DominatedCounts));
    await groupWriter.WriteColumnAsync(
        new DataColumn(Schemas.Dominators.DataFields[4], Enumerable.Repeat(runtimeId, dominatorData.ObjectAddresses.Length).ToArray()));
}

static async Task WriteObjectBlobToDatabaseAsync(ObjectsDb objectsDb, ClrObject obj, DataTarget dataTarget)
{
    if (obj.Size <= 0 || obj.Size >= int.MaxValue) return;

    int size = (int)obj.Size;
    byte[] buffer = new byte[size];
    int read = 0;
    try
    {
        read = dataTarget.DataReader.Read(obj.Address, buffer);
    }
    catch
    {
        read = 0;
    }

    if (read > 0)
    {
        await objectsDb.AddObjectBlobAsync(obj.Address, buffer);
    }
}

static async Task WriteStringToDatabaseAsync(StringsDb stringsDb, ClrObject obj)
{
    const int MaxStringSize = 1_000_000_000; // Max for sqlite.

    string? value = null;
    try
    {
        value = obj.AsString(MaxStringSize);
    }
    catch
    {
        value = null;
    }
    if (!string.IsNullOrEmpty(value))
    {
        await stringsDb.AddStringAsync(obj.Address, value);
    }
}

return 0;
