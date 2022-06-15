package unified

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// setCreateKeyDKO will parse an options document and set the data on an options.DataKeyOptions instance.
func setCreateKeyDKO(dko *options.DataKeyOptions, optsDocElem bson.RawElement) error {
	optsDoc, err := optsDocElem.Value().Document().Elements()
	if err != nil {
		return err
	}
	for _, elem := range optsDoc {
		key := elem.Key()
		val := elem.Value()
		switch key {
		case "masterKey":
			masterKey := make(map[string]interface{})
			if err := val.Unmarshal(&masterKey); err != nil {
				return fmt.Errorf("error unmarshaling 'masterKey': %v", err)
			}
			dko.SetMasterKey(masterKey)
		case "keyAltNames":
			keyAltNames := []string{}
			if err := val.Unmarshal(&keyAltNames); err != nil {
				return fmt.Errorf("error unmarshaling 'keyAltNames': %v", err)
			}
			dko.SetKeyAltNames(keyAltNames)
		case "keyMaterial":
			bin := primitive.Binary{}
			if err := val.Unmarshal(&bin); err != nil {
				return fmt.Errorf("error unmarshaling 'keyMaterial': %v", err)
			}
			dko.SetKeyMaterial(bin.Data)
		default:
			return fmt.Errorf("unrecognized DataKeyOptions arg: %q", key)
		}
	}
	return nil
}

// executeCreateKey will attempt to create a client-encrypted key for a unified operation.
func executeCreateKey(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}

	var kmsProvider string
	dko := options.DataKey()

	elems, err := operation.Arguments.Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "kmsProvider":
			kmsProvider = val.StringValue()
		case "opts":
			if err := setCreateKeyDKO(dko, elem); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unrecognized CreateKey arg: %q", key)
		}
	}
	if kmsProvider == "" {
		return nil, newMissingArgumentError("kmsProvider")
	}

	bin, err := cee.CreateKey(ctx, kmsProvider, dko)
	return newValueResult(bsontype.Binary, bin.Data, err), nil
}

// executeDeleteKey removes the key document with the given UUID (BSON binary subtype 0x04) from the key vault
// collection. Returns the result of the internal deleteOne() operation on the key vault collection.
func executeDeleteKey(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}

	var id primitive.Binary

	elems, err := operation.Arguments.Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "id":
			subtype, data := val.Binary()
			id = primitive.Binary{Subtype: subtype, Data: data}
		default:
			return nil, fmt.Errorf("unrecognized DeleteKey arg: %q", key)
		}
	}

	res, err := cee.DeleteKey(ctx, id)
	raw := emptyCoreDocument
	if res != nil {
		raw = bsoncore.NewDocumentBuilder().
			AppendInt64("deletedCount", res.DeletedCount).
			Build()
	}
	return newDocumentResult(raw, err), nil
}

// executeGetKeyByAltName returns a key document in the key vault collection with the given keyAltName.
func executeGetKeyByAltName(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}

	var keyAltName string

	elems, err := operation.Arguments.Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "keyAltName":
			keyAltName = val.StringValue()
		default:
			return nil, fmt.Errorf("unrecognized GetKeyByAltName arg: %q", key)
		}
	}

	res, err := cee.GetKeyByAltName(ctx, keyAltName).DecodeBytes()
	// Ignore ErrNoDocuments errors from DecodeBytes. In the event that the cursor returned in a find operation has no
	// associated documents, DecodeBytes will return ErrNoDocuments.
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return newDocumentResult(res, err), nil
}

// executeGetKey finds a single key document with the given UUID (BSON binary subtype 0x04). Returns the result of the
// internal find() operation on the key vault collection.
func executeGetKey(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}

	var id primitive.Binary

	elems, err := operation.Arguments.Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "id":
			subtype, data := val.Binary()
			id = primitive.Binary{Subtype: subtype, Data: data}
		default:
			return nil, fmt.Errorf("unrecognized GetKey arg: %q", key)
		}
	}

	res, err := cee.GetKey(ctx, id).DecodeBytes()
	// Ignore ErrNoDocuments errors from DecodeBytes. In the event that the cursor returned in a find operation has no
	// associated documents, DecodeBytes will return ErrNoDocuments.
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return newDocumentResult(res, err), nil
}

// executeGetKeys finds all documents in the key vault collection. Returns the result of the internal find() operation
// on the key vault collection.
func executeGetKeys(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}
	cursor, err := cee.GetKeys(ctx)
	if err != nil {
		return newErrorResult(err), nil
	}
	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		return newErrorResult(err), nil
	}
	return newCursorResult(docs), nil
}

// executeRemoveKeyAltName will remove keyAltName from the data key if it is present on the data key.
func executeRemoveKeyAltName(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}

	var id primitive.Binary
	var keyAltName string

	elems, err := operation.Arguments.Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "id":
			subtype, data := val.Binary()
			id = primitive.Binary{Subtype: subtype, Data: data}
		case "keyAltName":
			keyAltName = val.StringValue()
		default:
			return nil, fmt.Errorf("unrecognized RemoveKeyAltName arg: %q", key)
		}
	}

	res, err := cee.RemoveKeyAltName(ctx, id, keyAltName).DecodeBytes()
	// Ignore ErrNoDocuments errors from DecodeBytes. In the event that the cursor returned in a find operation has no
	// associated documents, DecodeBytes will return ErrNoDocuments.
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return newDocumentResult(res, err), nil
}

// setRewrapManyDataKeyOptions will parse an options document and set the data on an
// options.RewrapManyDataKeyOptions instance.
func setRewrapManyDataKeyOptions(rmdko *options.RewrapManyDataKeyOptions, optsDocElem bson.RawElement) error {
	optsDoc, err := optsDocElem.Value().Document().Elements()
	if err != nil {
		return err
	}
	for _, elem := range optsDoc {
		key := elem.Key()
		val := elem.Value()
		switch key {
		case "provider":
			rmdko.SetProvider(val.StringValue())
		case "masterKey":
			rmdko.SetMasterKey(val.Document())
		default:
			return fmt.Errorf("unrecognized RewrapManyDataKeyOptions arg: %q", key)
		}
	}
	return nil
}

// rewrapManyDataKeyResultsOpResult will wrap the result of rewrapping a data key into an operation result for test
// validation.
func rewrapManyDataKeyResultsOpResult(result *mongo.RewrapManyDataKeyResult) (*operationResult, error) {
	bulkWriteResult := bsoncore.NewDocumentBuilder()
	if res := result.BulkWriteResult; res != nil {
		rawUpsertedIDs := emptyDocument
		var marshalErr error
		if res.UpsertedIDs != nil {
			rawUpsertedIDs, marshalErr = bson.Marshal(res.UpsertedIDs)
			if marshalErr != nil {
				return nil, fmt.Errorf("error marshalling UpsertedIDs map to BSON: %v", marshalErr)
			}
		}
		bulkWriteResult.
			AppendInt64("insertedCount", res.InsertedCount).
			AppendInt64("deletedCount", res.DeletedCount).
			AppendInt64("matchedCount", res.MatchedCount).
			AppendInt64("modifiedCount", res.ModifiedCount).
			AppendInt64("upsertedCount", res.UpsertedCount).
			AppendDocument("upsertedIds", rawUpsertedIDs)
	}

	raw := bsoncore.NewDocumentBuilder().
		AppendDocument("bulkWriteResult", bulkWriteResult.Build()).
		Build()
	return newDocumentResult(raw, nil), nil
}

// executiveRewrapManyDataKey will attempt to re-wrap a number of data keys given a new provider,
// master key, and filter.
func executeRewrapManyDataKey(ctx context.Context, operation *operation) (*operationResult, error) {
	cee, err := entities(ctx).clientEncryption(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	rmdko := options.RewrapManyDataKey()
	elems, err := operation.Arguments.Elements()
	if err != nil {
		return nil, err
	}

	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "filter":
			filter = val.Document()
		case "opts":
			if err := setRewrapManyDataKeyOptions(rmdko, elem); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unrecognized RewrapManyDataKey arg: %q", key)
		}
	}

	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	result, err := cee.RewrapManyDataKey(ctx, filter, rmdko)
	if err != nil {
		return newErrorResult(err), nil
	}
	return rewrapManyDataKeyResultsOpResult(result)
}
