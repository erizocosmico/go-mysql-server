package expression

import (
	"regexp"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Comparison is an expression that compares an expression against another.
type Comparison struct {
	BinaryExpression
	// ChildType is the type of the resultant value after the comparison.
	ChildType sql.Type
}

// Type implements the Expression interface.
func (*Comparison) Type() sql.Type {
	return sql.Boolean
}

// Name implements the Expression interface.
func (*Comparison) Name() string {
	return ""
}

// Equals is a comparison that checks an expression is equal to another.
type Equals struct {
	Comparison
}

// NewEquals returns a new Equals expression.
func NewEquals(left sql.Expression, right sql.Expression) *Equals {
	return &Equals{Comparison{BinaryExpression{left, right}, left.Type()}}
}

// Eval implements the Expression interface.
func (e Equals) Eval(row sql.Row) (interface{}, error) {
	a, err := e.Left.Eval(row)
	if err != nil {
		return nil, err
	}

	b, err := e.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return e.ChildType.Compare(a, b) == 0, nil
}

// TransformUp implements the Expression interface.
func (e *Equals) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	lc := e.BinaryExpression.Left.TransformUp(f)
	rc := e.BinaryExpression.Right.TransformUp(f)

	return f(NewEquals(lc, rc))
}

// Name implements the Expression interface.
func (e Equals) Name() string {
	return e.Left.Name() + "==" + e.Right.Name()
}

// Regexp is a comparison that checks an expression matches a regexp.
type Regexp struct {
	Comparison
}

// NewRegexp creates a new Regexp expression.
func NewRegexp(left sql.Expression, right sql.Expression) *Regexp {
	return &Regexp{Comparison{BinaryExpression{left, right}, left.Type()}}
}

// Eval implements the Expression interface.
func (re Regexp) Eval(row sql.Row) (interface{}, error) {
	l, err := re.Left.Eval(row)
	if err != nil {
		return nil, err
	}
	r, err := re.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	if l == nil || r == nil {
		return nil, nil
	}

	sl, okl := l.(string)
	sr, okr := r.(string)

	if !okl || !okr {
		return re.ChildType.Compare(l, r) == 0, nil
	}

	reg, err := regexp.Compile(sr)
	if err != nil {
		return false, nil
	}

	return reg.MatchString(sl), nil
}

// TransformUp implements the Expression interface.
func (re *Regexp) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	lc := re.BinaryExpression.Left.TransformUp(f)
	rc := re.BinaryExpression.Right.TransformUp(f)

	return f(NewRegexp(lc, rc))
}

// Name implements the Expression interface.
func (re Regexp) Name() string {
	return re.Left.Name() + " REGEXP " + re.Right.Name()
}

// GreaterThan is a comparison that checks an expression is greater than another.
type GreaterThan struct {
	Comparison
}

// NewGreaterThan creates a new GreaterThan expression.
func NewGreaterThan(left sql.Expression, right sql.Expression) *GreaterThan {
	return &GreaterThan{Comparison{BinaryExpression{left, right}, left.Type()}}
}

// Eval implements the Expression interface.
func (gt GreaterThan) Eval(row sql.Row) (interface{}, error) {
	a, err := gt.Left.Eval(row)
	if err != nil {
		return nil, err
	}
	b, err := gt.Right.Eval(row)
	if err != nil {
		return nil, err
	}
	if a == nil || b == nil {
		return nil, nil
	}

	return gt.ChildType.Compare(a, b) == 1, nil
}

// TransformUp implements the Expression interface.
func (gt *GreaterThan) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	lc := gt.BinaryExpression.Left.TransformUp(f)
	rc := gt.BinaryExpression.Right.TransformUp(f)

	return f(NewGreaterThan(lc, rc))
}

// LessThan is a comparison that checks an expression is less than another.
type LessThan struct {
	Comparison
}

// NewLessThan creates a new LessThan expression.
func NewLessThan(left sql.Expression, right sql.Expression) *LessThan {
	return &LessThan{Comparison{BinaryExpression{left, right}, left.Type()}}
}

// Eval implements the expression interface.
func (lt LessThan) Eval(row sql.Row) (interface{}, error) {
	a, err := lt.Left.Eval(row)
	if err != nil {
		return nil, err
	}

	b, err := lt.Right.Eval(row)
	if err != nil {
		return nil, err
	}
	if a == nil || b == nil {
		return nil, nil
	}

	return lt.ChildType.Compare(a, b) == -1, nil
}

// TransformUp implements the Expression interface.
func (lt *LessThan) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	lc := lt.BinaryExpression.Left.TransformUp(f)
	rc := lt.BinaryExpression.Right.TransformUp(f)

	return f(NewLessThan(lc, rc))
}

// GreaterThanOrEqual is a comparison that checks an expression is greater or equal to
// another.
type GreaterThanOrEqual struct {
	Comparison
}

// NewGreaterThanOrEqual creates a new GreaterThanOrEqual
func NewGreaterThanOrEqual(left sql.Expression, right sql.Expression) *GreaterThanOrEqual {
	return &GreaterThanOrEqual{Comparison{BinaryExpression{left, right}, left.Type()}}
}

// Eval implements the Expression interface.
func (gte GreaterThanOrEqual) Eval(row sql.Row) (interface{}, error) {
	a, err := gte.Left.Eval(row)
	if err != nil {
		return nil, err
	}

	b, err := gte.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return gte.ChildType.Compare(a, b) > -1, nil
}

// TransformUp implements the Expression interface.
func (gte *GreaterThanOrEqual) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	lc := gte.BinaryExpression.Left.TransformUp(f)
	rc := gte.BinaryExpression.Right.TransformUp(f)

	return f(NewGreaterThanOrEqual(lc, rc))
}

// LessThanOrEqual is a comparison that checks an expression is equal or lower than
// another.
type LessThanOrEqual struct {
	Comparison
}

// NewLessThanOrEqual creates a LessThanOrEqual expression.
func NewLessThanOrEqual(left sql.Expression, right sql.Expression) *LessThanOrEqual {
	return &LessThanOrEqual{Comparison{BinaryExpression{left, right}, left.Type()}}
}

// Eval implements the Expression interface.
func (lte LessThanOrEqual) Eval(row sql.Row) (interface{}, error) {
	a, err := lte.Left.Eval(row)
	if err != nil {
		return nil, err
	}
	b, err := lte.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	if a == nil || b == nil {
		return nil, nil
	}

	return lte.ChildType.Compare(a, b) < 1, nil
}

// TransformUp implements the Expression interface.
func (lte *LessThanOrEqual) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	lc := lte.BinaryExpression.Left.TransformUp(f)
	rc := lte.BinaryExpression.Right.TransformUp(f)

	return f(NewLessThanOrEqual(lc, rc))
}
