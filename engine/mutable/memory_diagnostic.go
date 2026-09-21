//go:build memdiag

package mutable

// DiagnosticMemory reports owned TSSTORE column buffers. Metadata, allocator
// rounding and index memory are deliberately excluded; heap profiles cover them.
type DiagnosticMemory struct {
	LogicalBytes      int64
	Measurements      int
	Series            int
	Rows              int
	UnorderedRecords  int
	ValueLen          int
	ValueCap          int
	OffsetLenBytes    int
	OffsetCapBytes    int
	BitmapLen         int
	BitmapCap         int
	MaxColumnValueCap int
}

func (t *MemTable) DiagnosticMemory() DiagnosticMemory {
	var d DiagnosticMemory
	if t == nil {
		return d
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	d.LogicalBytes = t.GetMemSize()
	d.Measurements = len(t.msInfoMap)
	for _, ms := range t.msInfoMap {
		ms.mu.RLock()
		for _, chunk := range ms.sidMap {
			chunk.Mu.Lock()
			rec := chunk.WriteRec.rec
			if rec != nil {
				d.Series++
				d.Rows += rec.RowNums()
				if !chunk.WriteRec.timeAsd {
					d.UnorderedRecords++
				}
				for i := range rec.ColVals {
					col := &rec.ColVals[i]
					d.ValueLen += len(col.Val)
					d.ValueCap += cap(col.Val)
					d.OffsetLenBytes += len(col.Offset) * 4
					d.OffsetCapBytes += cap(col.Offset) * 4
					d.BitmapLen += len(col.Bitmap)
					d.BitmapCap += cap(col.Bitmap)
					d.MaxColumnValueCap = max(d.MaxColumnValueCap, cap(col.Val))
				}
			}
			chunk.Mu.Unlock()
		}
		ms.mu.RUnlock()
	}
	return d
}
