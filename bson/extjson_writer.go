package bson

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/mongodb/mongo-go-driver/bson/decimal"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
)

type ejvwState struct {
	mode mode
}

type extJSONValueWriter struct {
	w   io.Writer
	buf []byte

	stack     []ejvwState
	frame     int64
	canonical bool
}

// NewExtJSONValueWriter creates a ValueWriter that writes Extended JSON to w.
func NewExtJSONValueWriter(w io.Writer, canonical bool) (ValueWriter, error) {
	if w == nil {
		return nil, errNilWriter
	}

	return newExtJSONWriter(w, canonical), nil
}

func newExtJSONWriter(w io.Writer, canonical bool) *extJSONValueWriter {
	stack := make([]ejvwState, 1, 5)
	stack[0] = ejvwState{mode: mTopLevel}

	return &extJSONValueWriter{
		w:         w,
		buf:       []byte{},
		stack:     stack,
		canonical: canonical,
	}
}

func (ejvw *extJSONValueWriter) advanceFrame() {
	if ejvw.frame+1 >= int64(len(ejvw.stack)) { // We need to grow the stack
		length := len(ejvw.stack)
		if length+1 >= cap(ejvw.stack) {
			// double it
			buf := make([]ejvwState, 2*cap(ejvw.stack)+1)
			copy(buf, ejvw.stack)
			ejvw.stack = buf
		}
		ejvw.stack = ejvw.stack[:length+1]
	}
	ejvw.frame++
}

func (ejvw *extJSONValueWriter) push(m mode) {
	ejvw.advanceFrame()

	ejvw.stack[ejvw.frame].mode = m
}

func (ejvw *extJSONValueWriter) pop() {
	switch ejvw.stack[ejvw.frame].mode {
	case mElement, mValue:
		ejvw.frame--
	case mDocument, mArray, mCodeWithScope:
		ejvw.frame -= 2 // we pop twice to jump over the mElement: mDocument -> mElement -> mDocument/mTopLevel/etc...
	}
}

func (ejvw *extJSONValueWriter) invalidTransitionErr(destination mode) error {
	te := transitionError{
		current:     ejvw.stack[ejvw.frame].mode,
		destination: destination,
	}
	if ejvw.frame != 0 {
		te.parent = ejvw.stack[ejvw.frame-1].mode
	}
	return te
}

func (ejvw *extJSONValueWriter) ensureElementValue(destination mode) error {
	switch ejvw.stack[ejvw.frame].mode {
	case mElement, mValue:
	default:
		return ejvw.invalidTransitionErr(destination)
	}

	return nil
}

func (ejvw *extJSONValueWriter) writeExtendedSingleValue(key string, value string, quotes bool) {
	var s string
	if quotes {
		s = fmt.Sprintf(`{"$%s":"%s"},`, key, value)
	} else {
		s = fmt.Sprintf(`{"$%s":%s},`, key, value)
	}

	ejvw.buf = append(ejvw.buf, []byte(s)...)
}

func (ejvw *extJSONValueWriter) WriteArray() (ArrayWriter, error) {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return nil, err
	}

	ejvw.buf = append(ejvw.buf, '[')

	ejvw.push(mArray)
	return ejvw, nil
}

func (ejvw *extJSONValueWriter) WriteBinary(b []byte) error {
	return ejvw.WriteBinaryWithSubtype(b, 0x00)
}

func (ejvw *extJSONValueWriter) WriteBinaryWithSubtype(b []byte, btype byte) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteString(`{"$binary":{"base64":"`)
	buf.Write(b)
	buf.WriteString(`","subType":"`)
	buf.WriteByte(btype)
	buf.WriteString(`"}},`)

	ejvw.buf = append(ejvw.buf, buf.Bytes()...)

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteBoolean(b bool) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.buf = append(ejvw.buf, []byte(strconv.FormatBool(b))...)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteCodeWithScope(code string) (DocumentWriter, error) {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString(`{"$code":"`)
	buf.WriteString(code)
	buf.WriteString(`","$scope":{`)

	ejvw.buf = append(ejvw.buf, buf.Bytes()...)

	ejvw.push(mCodeWithScope)
	return ejvw, nil
}

func (ejvw *extJSONValueWriter) WriteDBPointer(ns string, oid objectid.ObjectID) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteString(`{"$dbPointer":{"$ref":"`)
	buf.WriteString(ns)
	buf.WriteString(`","$id":{"$oid":"`)
	buf.WriteString(oid.Hex())
	buf.WriteString(`"}}},`)

	ejvw.buf = append(ejvw.buf, buf.Bytes()...)

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteDateTime(dt int64) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	t := time.Unix(dt/1e3, dt%1e3*1e6)

	if ejvw.canonical || t.Year() < 1970 || t.Year() > 9999 {
		s := fmt.Sprintf(`{"$numberLong":"%d"},`, dt)
		ejvw.writeExtendedSingleValue("date", s, false)
	} else {
		ejvw.writeExtendedSingleValue("date", t.Format(rfc3339Milli), true)
	}

	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteDecimal128(d decimal.Decimal128) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("numberDecimal", d.String(), true)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteDocument() (DocumentWriter, error) {
	if ejvw.stack[ejvw.frame].mode == mTopLevel {
		ejvw.buf = append(ejvw.buf, '{')
		return ejvw, nil
	}

	if err := ejvw.ensureElementValue(mDocument); err != nil {
		return nil, err
	}

	ejvw.buf = append(ejvw.buf, '{')
	ejvw.push(mDocument)
	return ejvw, nil
}

func (ejvw *extJSONValueWriter) WriteDouble(f float64) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	s := strconv.FormatFloat(f, 'G', -1, 64)

	if ejvw.canonical {
		ejvw.writeExtendedSingleValue("numberDouble", s, true)
	} else {
		ejvw.buf = append(ejvw.buf, []byte(s)...)
	}

	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteInt32(i int32) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	s := strconv.FormatInt(int64(i), 10)

	if ejvw.canonical {
		ejvw.writeExtendedSingleValue("numberInt", s, true)
	} else {
		ejvw.buf = append(ejvw.buf, []byte(s)...)
	}

	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteInt64(i int64) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	s := strconv.FormatInt(i, 10)

	if ejvw.canonical {
		ejvw.writeExtendedSingleValue("numberLong", s, true)
	} else {
		ejvw.buf = append(ejvw.buf, []byte(s)...)
	}

	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteJavascript(code string) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("code", code, true)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteMaxKey() error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("maxKey", "1", false)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteMinKey() error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("minKey", "1", false)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteNull() error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.buf = append(ejvw.buf, []byte("null")...)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteObjectID(oid objectid.ObjectID) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("oid", oid.Hex(), true)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteRegex(pattern string, options string) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteString(`{"$regularExpression":{"pattern":"`)
	buf.WriteString(pattern)
	buf.WriteString(`","options":"`)
	buf.WriteString(options)
	buf.WriteString(`"}},`)

	ejvw.buf = append(ejvw.buf, buf.Bytes()...)

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteString(s string) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.buf = append(ejvw.buf, []byte(s)...)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteSymbol(symbol string) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("symbol", symbol, true)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteTimestamp(t uint32, i uint32) error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteString(`{"$timestamp":{"t":`)
	buf.WriteString(strconv.FormatUint(uint64(t), 10))
	buf.WriteString(`,"i":`)
	buf.WriteString(strconv.FormatUint(uint64(i), 10))
	buf.WriteString(`}},`)

	ejvw.buf = append(ejvw.buf, buf.Bytes()...)

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteUndefined() error {
	if err := ejvw.ensureElementValue(mode(0)); err != nil {
		return err
	}

	ejvw.writeExtendedSingleValue("undefined", "true", false)
	ejvw.buf = append(ejvw.buf, ',')

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteDocumentElement(key string) (ValueWriter, error) {
	switch ejvw.stack[ejvw.frame].mode {
	case mDocument, mTopLevel, mCodeWithScope:
		ejvw.buf = append(ejvw.buf, []byte(fmt.Sprintf(`"%s":`, key))...)
		ejvw.push(mElement)
	default:
		return nil, ejvw.invalidTransitionErr(mElement)
	}

	return ejvw, nil
}

func (ejvw *extJSONValueWriter) WriteDocumentEnd() error {
	switch ejvw.stack[ejvw.frame].mode {
	case mDocument, mTopLevel, mCodeWithScope:
	default:
		return fmt.Errorf("incorrect mode to end document: %s", ejvw.stack[ejvw.frame].mode)
	}

	// close the document
	if ejvw.buf[len(ejvw.buf)-1] == ',' {
		ejvw.buf[len(ejvw.buf)-1] = '}'
	} else {
		ejvw.buf = append(ejvw.buf, '}')
	}

	switch ejvw.stack[ejvw.frame].mode {
	case mCodeWithScope:
		ejvw.buf = append(ejvw.buf, '}')
		fallthrough
	case mDocument:
		ejvw.buf = append(ejvw.buf, ',')
	case mTopLevel:
		if _, err := ejvw.w.Write(ejvw.buf); err != nil {
			return err
		}

		ejvw.buf = ejvw.buf[:0]
	}

	ejvw.pop()
	return nil
}

func (ejvw *extJSONValueWriter) WriteArrayElement() (ValueWriter, error) {
	switch ejvw.stack[ejvw.frame].mode {
	case mArray:
		ejvw.push(mValue)
	default:
		return nil, ejvw.invalidTransitionErr(mValue)
	}

	return ejvw, nil
}

func (ejvw *extJSONValueWriter) WriteArrayEnd() error {
	switch ejvw.stack[ejvw.frame].mode {
	case mArray:
		// close the array
		if ejvw.buf[len(ejvw.buf)-1] == ',' {
			ejvw.buf[len(ejvw.buf)-1] = ']'
		} else {
			ejvw.buf = append(ejvw.buf, ']')
		}

		ejvw.pop()
	default:
		return fmt.Errorf("incorret mode to end array: %s", ejvw.stack[ejvw.frame].mode)
	}

	return nil
}
