using Microsoft.Data.Sqlite;

internal class StringsDb : IAsyncDisposable
{
    private readonly SqliteConnection _connection;
    private readonly SqliteTransaction _transaction;
    private readonly SqliteCommand _insertCommand;
    private readonly SqliteParameter _addressParam;
    private readonly SqliteParameter _stringParam;

    public StringsDb(string dbPath)
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
            createTableCmd.CommandText = "CREATE TABLE IF NOT EXISTS strings(address INTEGER PRIMARY KEY, value TEXT);";
            createTableCmd.ExecuteNonQuery();
        }

        _insertCommand = _connection.CreateCommand();
        _insertCommand.CommandText = "INSERT OR REPLACE INTO strings(address, value) VALUES ($address, $value);";
        _addressParam = _insertCommand.Parameters.Add("$address", SqliteType.Integer);
        _stringParam = _insertCommand.Parameters.Add("$value", SqliteType.Text);
    }

    public Task AddStringAsync(ulong address, string value)
    {
        _addressParam.Value = unchecked((long)address);
        _stringParam.Value = value;
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
