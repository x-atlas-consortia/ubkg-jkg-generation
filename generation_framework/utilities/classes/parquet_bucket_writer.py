import pyarrow as pa
import pyarrow.parquet as pq

class ParquetBucketWriter:
    """
    Accumulates dict rows for one node/rel type and flushes them to a
    Parquet file incrementally.
    """
    def __init__(self, path, schema: pa.Schema,batch_size=10_000):

        # Parquet output path
        self.path = path
        # Parquet schema
        self.schema = schema
        # Field names in schema
        self.names = set(schema.names)
        # Write batch size
        self.batch_size = batch_size
        # Write buffer
        self.buffer = []

        self.writer = pq.ParquetWriter(path, schema)
        self.count = 0

    def add(self, row: dict):

        extra = row.keys() - self.names
        if extra:
            raise Exception(f"Unexpected field(s) for {self.path}: {sorted(extra)}")
        # Add row to write buffer.
        self.buffer.append(row)

        # Write batches to Parquet.
        if len(self.buffer) >= self.batch_size:
            self._flush()

    def _flush(self):

        if not self.buffer:
            return
        try:
            table = pa.Table.from_pylist(self.buffer, schema=self.schema)
        except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
            for field in self.schema:
                try:
                    pa.array([r.get(field.name) for r in self.buffer], type=field.type)
                except (pa.ArrowTypeError, pa.ArrowInvalid) as fe:
                    bad = next((r.get(field.name) for r in self.buffer
                                if not self._fits(r.get(field.name), field.type)), None)
                    raise Exception(
                        f"{self.path}: column '{field.name}' ({field.type}) failed: {fe}; sample value: {bad!r}") from e
            raise
        self.writer.write_table(table)
        self.count += len(self.buffer)
        self.buffer.clear()

    def _fits(self,value, typ):
        try:
            pa.array([value], type=typ)
            return True
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            return False
    def close(self):

        if self.writer is None:
            return
        self._flush()
        self.writer.close()
        self.writer = None
