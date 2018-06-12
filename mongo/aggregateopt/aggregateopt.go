package aggregateopt

import (
	"time"

	"reflect"

	"github.com/mongodb/mongo-go-driver/core/option"
)

var aggregateBundle = new(AggregateBundle)

// Aggregate is options for the aggregate() function
type Aggregate interface {
	aggregate()
	ConvertOption() option.Optioner
}

// AggregateBundle is a bundle of Aggregate options
type AggregateBundle struct {
	option Aggregate
	next   *AggregateBundle
}

// Implement the Aggregate interface
func (ab *AggregateBundle) aggregate() {}

// ConvertOption implements the Aggregate interface
func (ab *AggregateBundle) ConvertOption() option.Optioner { return nil }

// BundleAggregate bundles Aggregate options
func BundleAggregate(opts ...Aggregate) *AggregateBundle {
	head := aggregateBundle

	for _, opt := range opts {
		newBundle := AggregateBundle{
			option: opt,
			next:   head,
		}

		head = &newBundle
	}

	return head
}

// AllowDiskUse adds an option to allow aggregation stages to write to temporary files.
func (ab *AggregateBundle) AllowDiskUse(b bool) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptAllowDiskUse(b),
		next:   ab,
	}

	return bundle
}

// BatchSize adds an option to specify the number of documents to return in every batch.
func (ab *AggregateBundle) BatchSize(i int32) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptBatchSize(i),
		next:   ab,
	}

	return bundle
}

// BypassDocumentValidation adds an option to allow the write to opt-out of document-level validation.
func (ab *AggregateBundle) BypassDocumentValidation(b bool) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptBypassDocumentValidation(b),
		next:   ab,
	}

	return bundle
}

//Collation adds an option to specify a Collation.
func (ab *AggregateBundle) Collation(c option.Collation) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptCollation{Collation: &c},
		next:   ab,
	}

	return bundle
}

// MaxTime adds an option to specify the maximum amount of time to allow the query to run.
func (ab *AggregateBundle) MaxTime(d time.Duration) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptMaxTime(d),
		next:   ab,
	}

	return bundle
}

// Comment adds an option to specify a string to help trace the operation through the database profiler, currentOp, and logs.
func (ab *AggregateBundle) Comment(s string) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptComment(s),
		next:   ab,
	}

	return bundle
}

// Hint adds an option to specify the index to use for the aggregation.
func (ab *AggregateBundle) Hint(hint interface{}) *AggregateBundle {
	bundle := &AggregateBundle{
		option: OptHint{hint},
		next:   ab,
	}

	return bundle
}

// Calculates the total length of a bundle, accounting for nested bundles.
func (ab *AggregateBundle) bundleLength() int {
	if ab == nil {
		return 0
	}

	bundleLen := 0
	for ; ab != nil && ab.option != nil; ab = ab.next {
		if converted, ok := ab.option.(*AggregateBundle); ok {
			// nested bundle
			bundleLen += converted.bundleLength()
			continue
		}

		bundleLen++
	}

	return bundleLen
}

// Unbundle transforms a bundle into a slice of options
func (ab *AggregateBundle) Unbundle(deduplicate bool) ([]option.Optioner, error) {
	if ab == nil {
		return nil, nil
	}

	listLen := ab.bundleLength()

	options := make([]option.Optioner, listLen)
	index := listLen - 1

	for listHead := ab; listHead != nil && listHead.option != nil; listHead = listHead.next {
		// check for nested bundles
		// if the current option is a nested bundle, Unbundle it and add its options to the current array
		if converted, ok := listHead.option.(*AggregateBundle); ok {
			nestedOptions, err := converted.Unbundle(deduplicate)
			if err != nil {
				return nil, err
			}

			startIndex := index - len(nestedOptions) + 1 // where to start inserting nested options

			// add nested options in order
			for _, nestedOp := range nestedOptions {
				options[startIndex] = nestedOp
				startIndex++
			}
			index -= len(nestedOptions)
			continue
		}

		options[index] = listHead.option.ConvertOption()
		index--
	}

	if !deduplicate {
		return options, nil
	}

	// iterate backwards and make dedup slice
	optionsSet := make(map[reflect.Type]struct{})

	for i := listLen - 1; i >= 0; i-- {
		currOption := options[i]
		optionType := reflect.TypeOf(currOption)

		if _, ok := optionsSet[optionType]; ok {
			// option already found
			options = append(options[:i], options[i+1:]...)
			continue
		}

		optionsSet[optionType] = struct{}{}
	}

	return options, nil
}

// AllowDiskUse allows aggregation stages to write to temporary files.
func AllowDiskUse(b bool) OptAllowDiskUse {
	return OptAllowDiskUse(b)
}

// BatchSize specifies the number of documents to return in every batch.
func BatchSize(i int32) OptBatchSize {
	return OptBatchSize(i)
}

// BypassDocumentValidation allows the write to opt-out of document-level validation.
func BypassDocumentValidation(b bool) OptBypassDocumentValidation {
	return OptBypassDocumentValidation(b)
}

// Collation specifies a collation.
func Collation(c option.Collation) OptCollation {
	return OptCollation{Collation: &c}
}

// MaxTime specifies the maximum amount of time to allow the query to run.
func MaxTime(d time.Duration) OptMaxTime {
	return OptMaxTime(d)
}

// Comment allows users to specify a string to help trace the operation through the database profiler, currentOp, and logs.
func Comment(s string) OptComment {
	return OptComment(s)
}

// Hint specifies the index to use for the aggregation.
func Hint(hint interface{}) OptHint {
	return OptHint{hint}
}

// OptAllowDiskUse allows aggregation stages to write to temporary files.
type OptAllowDiskUse option.OptAllowDiskUse

func (OptAllowDiskUse) aggregate() {}

// ConvertOption implements the Aggregate interface
func (opt OptAllowDiskUse) ConvertOption() option.Optioner {
	return option.OptAllowDiskUse(opt)
}

// OptBatchSize specifies the number of documents to return in every batch.
type OptBatchSize option.OptBatchSize

func (OptBatchSize) aggregate() {}

// ConvertOption implements the Aggregate interface
func (opt OptBatchSize) ConvertOption() option.Optioner {
	return option.OptBatchSize(opt)
}

// OptBypassDocumentValidation allows the write to opt-out of document-level validation.
type OptBypassDocumentValidation option.OptBypassDocumentValidation

// ConvertOption implements the Aggregate interface
func (opt OptBypassDocumentValidation) ConvertOption() option.Optioner {
	return option.OptBypassDocumentValidation(opt)
}

func (OptBypassDocumentValidation) aggregate() {}

// OptCollation specifies a collation.
type OptCollation option.OptCollation

func (OptCollation) aggregate() {}

// ConvertOption implements the Aggregate interface
func (opt OptCollation) ConvertOption() option.Optioner {
	return option.OptCollation(opt)
}

// OptMaxTime specifies the maximum amount of time to allow the query to run.
type OptMaxTime option.OptMaxTime

func (OptMaxTime) aggregate() {}

// ConvertOption implements the Aggregate interface
func (opt OptMaxTime) ConvertOption() option.Optioner {
	return option.OptMaxTime(opt)
}

// OptComment allows users to specify a string to help trace the operation through the database profiler, currentOp, and logs.
type OptComment option.OptComment

func (OptComment) aggregate() {}

// ConvertOption implements the Aggregate interface
func (opt OptComment) ConvertOption() option.Optioner {
	return option.OptComment(opt)
}

// OptHint specifies the index to use for the aggregation.
type OptHint option.OptHint

func (OptHint) aggregate() {}

// ConvertOption implements the Aggregate interface
func (opt OptHint) ConvertOption() option.Optioner {
	return option.OptHint(opt)
}
