// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package filter translates List/Watch request filters into sqlite conditions.
package filter

import (
	"fmt"
	"strings"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/siderolabs/gen/xslices"
)

const (
	sqliteTrue  = "true"
	sqliteFalse = "false"
)

// CompileLabelQueries compiles label query into sqlite condition.
//
// The returned condition might not be exact match, it might skip
// some unsupported terms.
// So the original filtering should still be applied after fetching results from the DB.
func CompileLabelQueries(query resource.LabelQueries) string {
	result := strings.Join(xslices.Map(query, CompileLabelQuery), " OR ")

	if result == "" {
		return sqliteTrue
	}

	return result
}

// CompileLabelQuery compiles a single label query into sqlite condition.
func CompileLabelQuery(query resource.LabelQuery) string {
	var terms []string

	for _, t := range query.Terms {
		compiledTerm := CompileLabelQueryTerm(t)
		if compiledTerm != "" { // returns empty for unsupported terms.
			terms = append(terms, "("+compiledTerm+")")
		}
	}

	if len(terms) == 0 {
		return sqliteTrue
	}

	return "(" + strings.Join(terms, " AND ") + ")"
}

// quote the value to be used in sqlite query.
func quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// CompileLabelQueryTerm compiles a single label query term into sqlite condition.
func CompileLabelQueryTerm(term resource.LabelTerm) string {
	if strings.ContainsRune(term.Key, '"') {
		// we can't support escaping double quote in JSON path in sqlite
		return ""
	}

	// SQLite JSON path spec uses $."key" to access object fields.
	selector := "labels ->> " + quote(`$."`+term.Key+`"`)

	switch term.Op {
	case resource.LabelOpExists:
		if term.Invert {
			return selector + " IS NULL"
		}

		return selector + " IS NOT NULL"
	case resource.LabelOpEqual:
		if len(term.Value) == 0 {
			if term.Invert {
				return sqliteTrue
			}

			return sqliteFalse
		}

		if term.Invert {
			return selector + " != " + quote(term.Value[0])
		}

		return selector + " = " + quote(term.Value[0])
	case resource.LabelOpIn:
		if len(term.Value) == 0 {
			if term.Invert {
				return sqliteTrue
			}

			return sqliteFalse
		}

		quotedValues := xslices.Map(term.Value, quote)

		if term.Invert {
			return selector + " NOT IN (" + strings.Join(quotedValues, ", ") + ")"
		}

		return selector + " IN (" + strings.Join(quotedValues, ", ") + ")"
	case resource.LabelOpLTE, resource.LabelOpLT, resource.LabelOpLTNumeric, resource.LabelOpLTENumeric:
		// unsupported in sqlite filter
		return ""
	default:
		panic(fmt.Sprintf("unsupported label term operator: %v", term.Op))
	}
}
