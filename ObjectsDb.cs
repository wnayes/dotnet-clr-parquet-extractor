using Microsoft.Data.Sqlite;

internal class ObjectsDb : IAsyncDisposable
{
    private readonly SqliteConnection _connection;
    private readonly SqliteTransaction _transaction;
    private readonly SqliteCommand _insertCommand;
    private readonly SqliteParameter _addressParam;
    private readonly SqliteParameter _blobParam;

    public ObjectsDb(string dbPath)
    {
        if (File.Exists(dbPath))
        {
            File.Delete(dbPath);
        }

        string connectionString = $"Data Source={dbPath};";
        _connection = new SqliteConnection(connectionString);
        _connection.Open();

        _transaction = _connection.BeginTransaction();

        using (var createTableCmd = _connection.CreateCommand())
        {
            createTableCmd.CommandText = "CREATE TABLE IF NOT EXISTS objects_blob(address INTEGER PRIMARY KEY, blob BLOB);";
            createTableCmd.ExecuteNonQuery();
        }

        _insertCommand = _connection.CreateCommand();
        _insertCommand.CommandText = "INSERT OR REPLACE INTO objects_blob(address, blob) VALUES ($address, $blob);";
        _addressParam = _insertCommand.Parameters.Add("$address", SqliteType.Integer);
        _blobParam = _insertCommand.Parameters.Add("$blob", SqliteType.Blob);
    }

    public Task AddObjectBlobAsync(ulong address, byte[] blob)
    {
        _addressParam.Value = unchecked((long)address);
        _blobParam.Value = blob;
        return _insertCommand.ExecuteNonQueryAsync();
    }

    public ValueTask CompleteAndCloseAsync()
    {
        return DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _transaction.CommitAsync();
        _connection.Close();
        await _insertCommand.DisposeAsync();
        await _transaction.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
