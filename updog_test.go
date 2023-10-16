package updog

/*
func TestCreate(t *testing.T) {
	idx := NewIndexWriter()

	check := func(idx *IndexWriter) {
		require.NotNil(t, idx.schema.Columns["a"])
		require.NotNil(t, idx.schema.Columns["b"])
		require.NotNil(t, idx.schema.Columns["c"])
		require.Equal(t, 3, len(idx.schema.Columns))

		require.Equal(t, uint64(3), idx.getValueBitmap(getValueIndex("a", "1")).GetCardinality())
		require.Equal(t, uint64(2), idx.getValueBitmap(getValueIndex("b", "3")).GetCardinality())
		require.Equal(t, uint64(1), idx.getValueBitmap(getValueIndex("a", "2")).GetCardinality())
		require.Equal(t, uint64(1), idx.getValueBitmap(getValueIndex("c", "3")).GetCardinality())
		require.Equal(t, uint64(2), idx.getValueBitmap(getValueIndex("c", "4")).GetCardinality())
		require.Equal(t, uint64(0), idx.getValueBitmap(getValueIndex("a", "3")).GetCardinality())
	}

	idx.AddRow(map[string]string{"a": "1", "b": "2", "c": "3"})
	idx.AddRow(map[string]string{"a": "1", "b": "3", "c": "4"})
	idx.AddRow(map[string]string{"a": "1", "b": "3", "c": "4"})
	idx.AddRow(map[string]string{"a": "2"})

	check(idx)

	var buf bytes.Buffer

	require.NoError(t, idx.WriteTo(&buf))

	newIdx, err := NewIndexFromReader(&buf)
	require.NoError(t, err)
	require.NotNil(t, newIdx)

	check(newIdx)
}
*/
