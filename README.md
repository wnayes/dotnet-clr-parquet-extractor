# dotnet-clr-parquet-extractor

A small .NET script that extracts heap information from a .NET memory dump,
creating a set of parquet and sqlite files that represent the data. These can be
used to do queries or visualizations from software that would not be able to
directly operate on .NET heap data.
