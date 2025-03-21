#!/bin/bash

set -o errexit  # Exit the script with error if any of the commands fail

# Default fuzztime to 10m.
FUZZTIME=${FUZZTIME:-10m}

if [ -z "$PROJECT_DIRECTORY" ]; then
	echo "Please set PROJECT_DIRECTORY env variable."
    exit 1
fi

# Change the working directory to the root of the mongo repository directory
cd $PROJECT_DIRECTORY

# Get all go test files that contain a fuzz test.
FILES=$(grep -r --include='**_test.go' --files-with-matches 'func Fuzz' .)

# For each file, run all of the fuzz tests in sequence, each for -fuzztime=FUZZTIME.
for FILE in ${FILES}
do
	PARENTDIR="$(dirname -- "$FILE")"

	# Get a list of all fuzz tests in the file.
	FUNCS=$(grep -o 'func Fuzz[A-Za-z0-9]*' $FILE | cut -d' ' -f2)

	# For each fuzz test in the file, run it for FUZZTIME.
	for FUNC in ${FUNCS}
	do
		echo "Fuzzing \"${FUNC}\" in \"${FILE}\" for ${FUZZTIME}"

		# Create a set of directories that are already in the subdirectories testdata/fuzz/$fuzzer corpus. This
		# set will be used to differentiate between new and old corpus files.
		declare -a cset

		if [ -d $PARENTDIR/testdata/fuzz/$FUNC ]; then
			# Iterate over the files in the corpus directory and add them to the set.
			for SEED in $PARENTDIR/testdata/fuzz/$FUNC/*
			do
				cset+=("$SEED")
			done
		fi

		GOMAXPROCS=2 go test -race ${PARENTDIR} -run=${FUNC} -fuzz=^${FUNC}\$ -fuzztime=${FUZZTIME} || true

		# Check if any new corpus files were generated for the fuzzer. If there are new corpus files, move them
		# to $PROJECT_DIRECTORY/fuzz/$FUNC/* so they can be tarred up and uploaded to S3.
		if [ -d $PARENTDIR/testdata/fuzz/$FUNC ]; then
			# Iterate over the files in the corpus directory and check if they are in the set.
			for CORPUS_FILE in $PARENTDIR/testdata/fuzz/$FUNC/*
			do
				# Check to see if the value for CORPUS_FILE is in cset.
				# shellcheck disable=SC2076
				if [[ ! " ${cset[*]} " =~ " ${CORPUS_FILE} " ]]; then
					# Create the directory if it doesn't exist.
					if [ ! -d $PROJECT_DIRECTORY/fuzz/$FUNC ]; then
						mkdir -p $PROJECT_DIRECTORY/fuzz/$FUNC
					fi

					# Move the file to the directory.
					mv $CORPUS_FILE $PROJECT_DIRECTORY/fuzz/$FUNC

					echo "Moved $CORPUS_FILE to $PROJECT_DIRECTORY/fuzz/$FUNC"
				fi
			done
		fi
	done
done

# If the fuzz directory exists, then tar it up in preparation to upload to S3.
if [ -d $PROJECT_DIRECTORY/fuzz ]; then
	echo "Tarring up fuzz directory"

  	cd $PROJECT_DIRECTORY
  	tar cfz fuzz.tgz ./fuzz

	# Exit with code 1 to indicate that errors occurred in fuzz tests, resulting in corpus files being generated.
	# This will trigger a notification to be sent to the Go Driver team.
	exit 1
fi
