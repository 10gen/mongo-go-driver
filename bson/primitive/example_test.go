package primitive_test

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ExampleRegex() {
	ctx := context.TODO()
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to a mongodb server.
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		panic(err)
	}

	defer client.Disconnect(ctx)

	coll := client.Database("test").Collection("test")
	defer coll.Drop(ctx)

	// Create a slice of documents to insert. We will lookup a subset of
	// these documents using regex.
	toInsert := []interface{}{
		bson.D{{"foo", "bar"}},
		bson.D{{"foo", "baz"}},
		bson.D{{"foo", "qux"}},
	}

	if _, err := coll.InsertMany(ctx, toInsert); err != nil {
		panic(err)
	}

	// Create a filter to find a document with key "foo" and any value that
	// starts with letter "b".
	filter := bson.D{{"foo", primitive.Regex{Pattern: "^b", Options: ""}}}

	// Remove "_id" from the results.
	options := options.Find().SetProjection(bson.D{{"_id", 0}})

	_, err = coll.Find(ctx, filter, options)
	if err != nil {
		panic(err)
	}
}
