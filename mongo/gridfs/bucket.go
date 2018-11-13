// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package gridfs

import (
	"context"

	"io"

	"errors"

	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/mongodb/mongo-go-driver/core/readconcern"
	"github.com/mongodb/mongo-go-driver/core/readpref"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/options"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
)

// TODO: add sessions options

// DefaultChunkSize is the default size of each file chunk.
const DefaultChunkSize int32 = 255 * 1000 // 255 KB

// ErrFileNotFound occurs if a user asks to download a file with a file ID that isn't found in the files collection.
var ErrFileNotFound = errors.New("file with given parameters not found")

// Bucket represents a GridFS bucket.
type Bucket struct {
	db         *mongo.Database
	chunksColl *mongo.Collection // collection to store file chunks
	filesColl  *mongo.Collection // collection to store file metadata

	name      string
	chunkSize int32
	wc        *writeconcern.WriteConcern
	rc        *readconcern.ReadConcern
	rp        *readpref.ReadPref

	firstWriteDone bool
	readBuf        []byte
	writeBuf       []byte

	readDeadline  time.Time
	writeDeadline time.Time
}

// Upload contains options to upload a file to a bucket.
type Upload struct {
	chunkSize int32
	metadata  bsonx.Doc
}

// NewBucket creates a GridFS bucket.
func NewBucket(db *mongo.Database, opts ...*options.BucketOptions) (*Bucket, error) {
	b := &Bucket{
		name:      "fs",
		chunkSize: DefaultChunkSize,
		db:        db,
		wc:        db.WriteConcern(),
		rc:        db.ReadConcern(),
		rp:        db.ReadPreference(),
	}

	bo := options.MergeBucketOptions(opts...)
	if bo.Name != nil {
		b.name = *bo.Name
	}
	if bo.ChunkSizeBytes != nil {
		b.chunkSize = *bo.ChunkSizeBytes
	}
	if bo.WriteConcern != nil {
		b.wc = bo.WriteConcern
	}
	if bo.ReadConcern != nil {
		b.rc = bo.ReadConcern
	}
	if bo.ReadPreference != nil {
		b.rp = bo.ReadPreference
	}

	var collOpts = options.Collection().SetWriteConcern(b.wc).SetReadConcern(b.rc).SetReadPreference(b.rp)

	b.chunksColl = db.Collection(b.name+".chunks", collOpts)
	b.filesColl = db.Collection(b.name+".files", collOpts)
	b.readBuf = make([]byte, b.chunkSize)
	b.writeBuf = make([]byte, b.chunkSize)

	return b, nil
}

// SetWriteDeadline sets the write deadline for this bucket.
func (b *Bucket) SetWriteDeadline(t time.Time) error {
	b.writeDeadline = t
	return nil
}

// SetReadDeadline sets the read deadline for this bucket
func (b *Bucket) SetReadDeadline(t time.Time) error {
	b.readDeadline = t
	return nil
}

// OpenUploadStream creates a file ID new upload stream for a file given the filename.
func (b *Bucket) OpenUploadStream(filename string, opts ...*options.UploadOptions) (*UploadStream, error) {
	return b.OpenUploadStreamWithID(objectid.New(), filename, opts...)
}

// OpenUploadStreamWithID creates a new upload stream for a file given the file ID and filename.
func (b *Bucket) OpenUploadStreamWithID(fileID objectid.ObjectID, filename string, opts ...*options.UploadOptions) (*UploadStream, error) {
	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	if err := b.checkFirstWrite(ctx); err != nil {
		return nil, err
	}

	upload, err := b.parseUploadOptions(opts...)
	if err != nil {
		return nil, err
	}

	return newUploadStream(upload, fileID, filename, b.chunksColl, b.filesColl), nil
}

// UploadFromStream creates a fileID and uploads a file given a source stream.
func (b *Bucket) UploadFromStream(filename string, source io.Reader, opts ...*options.UploadOptions) (objectid.ObjectID, error) {
	fileID := objectid.New()
	err := b.UploadFromStreamWithID(fileID, filename, source, opts...)
	return fileID, err
}

// UploadFromStreamWithID uploads a file given a source stream.
func (b *Bucket) UploadFromStreamWithID(fileID objectid.ObjectID, filename string, source io.Reader, opts ...*options.UploadOptions) error {
	us, err := b.OpenUploadStreamWithID(fileID, filename, opts...)
	if err != nil {
		return err
	}

	err = us.SetWriteDeadline(b.writeDeadline)
	if err != nil {
		_ = us.Close()
		return err
	}

	for {
		n, err := source.Read(b.readBuf)
		if err != nil && err != io.EOF {
			_ = us.Abort() // upload considered aborted if source stream returns an error
			return err
		}

		if n > 0 {
			_, err := us.Write(b.readBuf[:n])
			if err != nil {
				return err
			}
		}

		if n == 0 || err == io.EOF {
			break
		}
	}

	return us.Close()
}

// OpenDownloadStream creates a stream from which the contents of the file can be read.
func (b *Bucket) OpenDownloadStream(fileID objectid.ObjectID) (*DownloadStream, error) {
	return b.openDownloadStream(bsonx.Doc{
		{"_id", bson.ObjectID(fileID)},
	})
}

// DownloadToStream downloads the file with the specified fileID and writes it to the provided io.Writer.
// Returns the number of bytes written to the steam and an error, or nil if there was no error.
func (b *Bucket) DownloadToStream(fileID objectid.ObjectID, stream io.Writer) (int64, error) {
	ds, err := b.OpenDownloadStream(fileID)
	if err != nil {
		return 0, err
	}

	return b.downloadToStream(ds, stream)
}

// OpenDownloadStreamByName opens a download stream for the file with the given filename.
func (b *Bucket) OpenDownloadStreamByName(filename string, opts ...*options.NameOptions) (*DownloadStream, error) {
	var numSkip int32 = -1
	var sortOrder int32 = 1

	nameOpts := options.MergeNameOptions(opts...)
	if nameOpts.Revision != nil {
		numSkip = *nameOpts.Revision
	}

	if numSkip < 0 {
		sortOrder = -1
		numSkip = (-1 * numSkip) - 1
	}

	findOpts := options.Find().SetSkip(int64(numSkip)).SetSort(bsonx.Doc{{"uploadDate", bson.Int32(sortOrder)}})

	return b.openDownloadStream(bsonx.Doc{{"filename", bson.String(filename)}}, findOpts)
}

// DownloadToStreamByName downloads the file with the given name to the given io.Writer.
func (b *Bucket) DownloadToStreamByName(filename string, stream io.Writer, opts ...*options.NameOptions) (int64, error) {
	ds, err := b.OpenDownloadStreamByName(filename, opts...)
	if err != nil {
		return 0, err
	}

	return b.downloadToStream(ds, stream)
}

// Delete deletes all chunks and metadata associated with the file with the given file ID.
func (b *Bucket) Delete(fileID objectid.ObjectID) error {
	// delete document in files collection and then chunks to minimize race conditions

	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	res, err := b.filesColl.DeleteOne(ctx, bsonx.Doc{{"_id", bson.ObjectID(fileID)}})
	if err == nil && res.DeletedCount == 0 {
		err = ErrFileNotFound
	}
	if err != nil {
		_ = b.deleteChunks(ctx, fileID) // can attempt to delete chunks even if no docs in files collection matched
		return err
	}

	return b.deleteChunks(ctx, fileID)
}

// Find returns the files collection documents that match the given filter.
func (b *Bucket) Find(filter interface{}, opts ...*options.GridFSFindOptions) (mongo.Cursor, error) {
	ctx, cancel := deadlineContext(b.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	gfsOpts := options.MergeGridFSFindOptions(opts...)
	find := options.Find()
	if gfsOpts.BatchSize != nil {
		find.SetBatchSize(*gfsOpts.BatchSize)
	}
	if gfsOpts.Limit != nil {
		find.SetLimit(int64(*gfsOpts.Limit))
	}
	if gfsOpts.MaxTime != nil {
		find.SetMaxTime(*gfsOpts.MaxTime)
	}
	if gfsOpts.NoCursorTimeout != nil {
		find.SetNoCursorTimeout(*gfsOpts.NoCursorTimeout)
	}
	if gfsOpts.Skip != nil {
		find.SetSkip(int64(*gfsOpts.Skip))
	}
	if gfsOpts.Sort != nil {
		find.SetSort(gfsOpts.Sort)
	}

	return b.filesColl.Find(ctx, filter, find)
}

// Rename renames the stored file with the specified file ID.
func (b *Bucket) Rename(fileID objectid.ObjectID, newFilename string) error {
	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	res, err := b.filesColl.UpdateOne(ctx,
		bsonx.Doc{{"_id", bson.ObjectID(fileID)}},
		bsonx.Doc{{"$set", bsonx.Document(bsonx.Doc{{"filename", bson.String(newFilename)}})}},
	)
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return ErrFileNotFound
	}

	return nil
}

// Drop drops the files and chunks collections associated with this bucket.
func (b *Bucket) Drop() error {
	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	err := b.filesColl.Drop(ctx)
	if err != nil {
		return err
	}

	return b.chunksColl.Drop(ctx)
}

func (b *Bucket) openDownloadStream(filter interface{}, opts ...*options.FindOptions) (*DownloadStream, error) {
	ctx, cancel := deadlineContext(b.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	cursor, err := b.findFile(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	fileRdr, err := cursor.DecodeBytes()
	if err != nil {
		return nil, err
	}

	fileLenElem, err := fileRdr.LookupErr("length")
	if err != nil {
		return nil, err
	}
	fileIDElem, err := fileRdr.LookupErr("_id")
	if err != nil {
		return nil, err
	}

	fileLen := fileLenElem.Int32()
	if fileLen == 0 {
		return newDownloadStream(nil, b.chunkSize, 0), nil
	}

	chunksCursor, err := b.findChunks(ctx, fileIDElem.ObjectID())
	if err != nil {
		return nil, err
	}
	return newDownloadStream(chunksCursor, b.chunkSize, int64(fileLen)), nil
}

func deadlineContext(deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.Equal(time.Time{}) {
		return context.Background(), nil
	}

	return context.WithDeadline(context.Background(), deadline)
}

func (b *Bucket) downloadToStream(ds *DownloadStream, stream io.Writer) (int64, error) {
	err := ds.SetReadDeadline(b.readDeadline)
	if err != nil {
		_ = ds.Close()
		return 0, err
	}

	copied, err := io.Copy(stream, ds)
	if err != nil {
		_ = ds.Close()
		return 0, err
	}

	return copied, ds.Close()
}

func (b *Bucket) deleteChunks(ctx context.Context, fileID objectid.ObjectID) error {
	_, err := b.chunksColl.DeleteMany(ctx, bsonx.Doc{{"files_id", bson.ObjectID(fileID)}})
	return err
}

func (b *Bucket) findFile(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (mongo.Cursor, error) {
	cursor, err := b.filesColl.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	if !cursor.Next(ctx) {
		_ = cursor.Close(ctx)
		return nil, ErrFileNotFound
	}

	return cursor, nil
}

func (b *Bucket) findChunks(ctx context.Context, fileID objectid.ObjectID) (mongo.Cursor, error) {
	chunksCursor, err := b.chunksColl.Find(ctx,
		bsonx.Doc{{"files_id", bson.ObjectID(fileID)}},
		options.Find().SetSort(bsonx.Doc{{"n", bson.Int32(1)}})) // sort by chunk index
	if err != nil {
		return nil, err
	}

	return chunksCursor, nil
}

// Create an index if it doesn't already exist
func createIndexIfNotExists(ctx context.Context, iv mongo.IndexView, model mongo.IndexModel) error {
	c, err := iv.List(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Close(ctx)
	}()

	var found bool
	for c.Next(ctx) {
		rdr, err := c.DecodeBytes()
		if err != nil {
			return err
		}

		keyElem, err := rdr.LookupErr("key")
		if err != nil {
			return err
		}

		keyElemDoc, err := bson.ReadDoc(keyElem.Document())
		if err != nil {
			return err
		}

		if model.Keys.Equal(keyElemDoc) {
			found = true
			break
		}
	}

	if !found {
		_, err = iv.CreateOne(ctx, model)
		if err != nil {
			return err
		}
	}

	return nil
}

// create indexes on the files and chunks collection if needed
func (b *Bucket) createIndexes(ctx context.Context) error {
	// must use primary read pref mode to check if files coll empty
	cloned, err := b.filesColl.Clone(options.Collection().SetReadPreference(readpref.Primary()))
	if err != nil {
		return err
	}

	docRes := cloned.FindOne(ctx, bsonx.Doc{}, options.FindOne().SetProjection(bsonx.Doc{{"_id", bson.Int32(1)}}))

	err = docRes.Decode(nil)
	if err == mongo.ErrNoDocuments {
		filesIv := b.filesColl.Indexes()
		chunksIv := b.chunksColl.Indexes()

		filesModel := mongo.IndexModel{
			Keys: bsonx.Doc{
				{"filename", bson.Int32(1)},
				{"uploadDate", bson.Int32(1)},
			},
		}

		chunksModel := mongo.IndexModel{
			Keys: bsonx.Doc{
				{"files_id", bson.Int32(1)},
				{"n", bson.Int32(1)},
			},
		}

		if err = createIndexIfNotExists(ctx, filesIv, filesModel); err != nil {
			return err
		}
		if err = createIndexIfNotExists(ctx, chunksIv, chunksModel); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) checkFirstWrite(ctx context.Context) error {
	if !b.firstWriteDone {
		// before the first write operation, must determine if files collection is empty
		// if so, create indexes if they do not already exist

		if err := b.createIndexes(ctx); err != nil {
			return err
		}
		b.firstWriteDone = true
	}

	return nil
}

func (b *Bucket) parseUploadOptions(opts ...*options.UploadOptions) (*Upload, error) {
	upload := &Upload{
		chunkSize: b.chunkSize, // upload chunk size defaults to bucket's value
	}

	uo := options.MergeUploadOptions(opts...)
	if uo.ChunkSizeBytes != nil {
		upload.chunkSize = *uo.ChunkSizeBytes
	}
	if uo.Metadata != nil {
		upload.metadata = uo.Metadata
	}

	return upload, nil
}
