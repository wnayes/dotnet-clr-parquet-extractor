# dotnet-clr-parquet-extractor

A small .NET script that extracts heap information from a .NET memory dump,
creating a set of parquet and sqlite files that represent the data. These can be
used to do queries or visualizations from software that would not be able to
directly operate on .NET heap data.

## Examples

### Load the parquet tables into DuckDB

```sql
CREATE VIEW dominators AS
SELECT * FROM 'dominators.parquet';

CREATE VIEW handles AS
SELECT * FROM 'handles.parquet';

CREATE VIEW modules AS
SELECT * FROM 'modules.parquet';

CREATE VIEW objects AS
SELECT * FROM 'objects.parquet';

CREATE VIEW refs AS
SELECT * FROM 'references.parquet';

CREATE VIEW roots AS
SELECT * FROM 'roots.parquet';

CREATE VIEW runtimes AS
SELECT * FROM 'runtimes.parquet';

CREATE VIEW static_fields AS
SELECT * FROM 'static_fields.parquet';

CREATE VIEW types AS
SELECT * FROM 'types.parquet';
```

### Load the sqlite tables into DuckDB

```sql
INSTALL sqlite;
LOAD sqlite;

ATTACH 'objects.sqlite' AS objects_db (TYPE sqlite);
ATTACH 'strings.sqlite' AS strings_db (TYPE sqlite);
```

### Basic `!dumpheap -stat` equivalent

```sql
SELECT
    t.type_name,
    COUNT(*)                    AS object_count,
    SUM(o.size_bytes)           AS total_size_bytes
FROM objects o
JOIN types t
  ON o.type_id = t.type_id
 AND o.runtime_id = t.runtime_id
GROUP BY t.type_name
ORDER BY total_size_bytes DESC;
```

### Basic `!dumpheap -stat` equivalent (including MethodTable)

```sql
SELECT
    t.method_table AS MT,
    COUNT(*)       AS Count,
    SUM(o.size_bytes) AS TotalSize,
    t.type_name    AS TypeName
FROM objects o
JOIN types t
  ON o.type_id = t.type_id
 AND o.runtime_id = t.runtime_id
GROUP BY t.method_table, t.type_name
ORDER BY TotalSize DESC;
```

### Identifying top retainers ("dominators")

```sql
SELECT
    o.object_id,
    t.type_name,
    d.dominated_size_bytes,
    d.dominated_count
FROM dominators d
JOIN objects o
  ON d.object_id = o.object_id
 AND d.runtime_id = o.runtime_id
JOIN types t
  ON o.type_id = t.type_id
 AND o.runtime_id = t.runtime_id
ORDER BY d.dominated_size_bytes DESC
LIMIT 50;
```

### Identifying largest static retainers

```sql
SELECT
    t.type_name,
    r.field_name,
    SUM(d.dominated_size_bytes) AS retained_bytes
FROM dominators d
JOIN refs r
  ON d.object_id = r.to_object_id
JOIN objects o
  ON r.from_object_id = o.object_id
JOIN types t
  ON o.type_id = t.type_id
JOIN roots rt
  ON r.from_object_id = rt.object_id
WHERE rt.root_kind = 4
GROUP BY t.type_name, r.field_name
ORDER BY retained_bytes DESC;
```
