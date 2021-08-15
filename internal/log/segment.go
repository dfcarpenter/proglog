package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/dfcarpenter/proglog/api/v1"
	"google.golang.org/protobuf/proto"

)


/*
The segment wraps the index and store types to coordinate operations across the two.
*/
type segment struct {
	store *store
	index *index
	baseOffset, nextOffset uint64
	config Config
}

/*
newSegment is called when the log needs to add a new segment such as when the current active segment hits its max size.

If the index is empty, then the next record appended to the segment would be the first record and its offset would
be the segments base offset.
*/
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config: c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil

}

/*
Append write the record to the segment and returns the cursor to the newly appended record's offset. The log returns
the offset to the API response. The segment appends a record in a two step process: it appends the data to the store
and then adds an index entry.
*/
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cursor := s.nextOffset
	record.Offset = cursor
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cursor, nil

}

/*
Read
*/
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

/*
IsMaxed returns whether the segment has reached its max size
If you wrote a small number of long logs then you'd hit the segment bytes limit; if you wrote a lot of small logs,
then you'd hit the index bytes limit.
*/
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

/*
nearestMultiple returns lesser multiple between two numbers to make sure we stay under
users disk capacity.
*/
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}