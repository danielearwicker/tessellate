﻿namespace Parquet.MapReduce.Types;

public class KeyMapping<SK, TK>
{
    public SK? SourceKey { get; set; }
    public TK? TargetKey { get; set; }
}
