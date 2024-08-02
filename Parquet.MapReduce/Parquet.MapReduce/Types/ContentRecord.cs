﻿namespace Parquet.MapReduce.Types;

public class ContentRecord<TK, SK, TV> : KeyMapping<SK, TK>
{
    public TV? Value;
}
