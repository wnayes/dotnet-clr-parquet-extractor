using Parquet.Schema;

internal static class Schemas
{
    public static readonly ParquetSchema Runtimes = new(
        new DataField<int>("Index"),
        new DataField<string>("Flavor"),
        new DataField<string>("Version")
    );

    public static readonly ParquetSchema Types = new(
        new DataField<int>("type_id"),
        new DataField<int>("runtime_id"),
        new DataField<ulong>("method_table"),
        new DataField<string>("type_name"),
        new DataField<bool>("is_array"),
        new DataField<bool>("is_string"),
        new DataField<bool>("is_value_type"),
        new DataField<int>("static_size_bytes")
    );

    public static readonly ParquetSchema Objects = new(
        new DataField<ulong>("object_id"),
        new DataField<int>("runtime_id"),
        new DataField<int>("type_id"),
        new DataField<ulong>("size_bytes"),
        new DataField<int>("generation")
    );

    public static readonly ParquetSchema References = new(
        new DataField<ulong>("from_object_id"),
        new DataField<ulong>("to_object_id"),
        new DataField<int>("runtime_id"),
        new DataField<string>("field_name")
    );

    public static readonly ParquetSchema Handles = new(
        new DataField<byte>("handle_kind"),
        new DataField<ulong>("address"),
        new DataField<ulong>("object_address"),
        new DataField<ulong>("dependent_address"),
        new DataField<uint>("reference_count"),
        new DataField<byte>("root_kind"),
        new DataField<bool>("is_interior"),
        new DataField<int>("runtime_id")
    );

    public static readonly ParquetSchema Roots = new(
        new DataField<ulong>("root_address"),
        new DataField<ulong>("object_id"),
        new DataField<byte>("root_kind"),
        new DataField<bool>("is_interior"),
        new DataField<int>("runtime_id")
    );

    public static readonly ParquetSchema Dominators = new(
        new DataField<ulong>("object_id"),
        new DataField<ulong>("immediate_dominator_id"),
        new DataField<ulong>("dominated_size_bytes"),
        new DataField<int>("dominated_count"),
        new DataField<int>("runtime_id")
    );

    public static readonly ParquetSchema Modules = new(
        new DataField<string>("file_name"),
        new DataField<bool>("is_managed"),
        new DataField<string>("kind"),
        new DataField<ulong>("image_base"),
        new DataField<long>("image_size"),
        new DataField<int>("index_file_size"),
        new DataField<string>("version")
    );
}
