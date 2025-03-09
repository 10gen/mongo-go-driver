// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func executeIterateOnce(ctx context.Context, operation *operation) (*operationResult, error) {
	cursor, err := entities(ctx).cursor(operation.Object)
	if err != nil {
		return nil, err
	}

	// TryNext will attempt to get the next document, potentially issuing a single 'getMore'.
	if cursor.TryNext(ctx) {
		// We don't expect the server to return malformed documents, so any errors from Decode here are treated
		// as fatal.
		var res bson.Raw
		if err := cursor.Decode(&res); err != nil {
			return nil, fmt.Errorf("error decoding cursor result: %w", err)
		}

		return newDocumentResult(res, nil), nil
	}

	return newErrorResult(cursor.Err()), nil
}

func executeIterateUntilDocumentOrError(ctx context.Context, operation *operation) (*operationResult, error) {
	cursor, err := entities(ctx).cursor(operation.Object)
	if err != nil {
		return nil, err
	}

	// Next will loop until there is either a result or an error.
	if cursor.Next(ctx) {
		// We don't expect the server to return malformed documents, so any errors from Decode are treated as fatal.
		var res bson.Raw
		if err := cursor.Decode(&res); err != nil {
			return nil, fmt.Errorf("error decoding cursor result: %w", err)
		}

		return newDocumentResult(res, nil), nil
	}
	return newErrorResult(cursor.Err()), nil
}
