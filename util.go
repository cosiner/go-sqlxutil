package sqlxutil

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

//==============================================================================
//                                Get
//==============================================================================
func GetContext(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args ...interface{}) error {
	return sqlx.GetContext(ctx, ext, ptr, sql, args...)
}

func Get(ext sqlx.Ext, ptr interface{}, sql string, args ...interface{}) error {
	return sqlx.Get(ext, ptr, sql, args...)
}

func RebindGetContext(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args ...interface{}) error {
	return GetContext(ctx, ext, ptr, ext.Rebind(sql), args...)
}

func RebindGet(ext sqlx.Ext, ptr interface{}, sql string, args ...interface{}) error {
	return Get(ext, ptr, ext.Rebind(sql), args...)
}

func NamedGetContext(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args interface{}) error {
	sql, argv, err := ext.BindNamed(sql, args)
	if err != nil {
		return err
	}
	return GetContext(ctx, ext, ptr, sql, argv...)
}

func NamedGet(ext sqlx.Ext, ptr interface{}, sql string, args interface{}) error {
	sql, argv, err := ext.BindNamed(sql, args)
	if err != nil {
		return err
	}
	return Get(ext, ptr, sql, argv...)
}

//==============================================================================
//                                Select
//==============================================================================
func SelectContext(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args ...interface{}) error {
	return sqlx.SelectContext(ctx, ext, ptr, sql, args...)
}

func Select(ext sqlx.Ext, ptr interface{}, sql string, args ...interface{}) error {
	return sqlx.Select(ext, ptr, sql, args...)
}

func RebindSelectContext(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args ...interface{}) error {
	return SelectContext(ctx, ext, ptr, ext.Rebind(sql), args...)
}

func RebindSelect(ext sqlx.Ext, ptr interface{}, sql string, args ...interface{}) error {
	return Select(ext, ptr, ext.Rebind(sql), args...)
}

func NamedSelectContext(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args interface{}) error {
	sql, argv, err := ext.BindNamed(sql, args)
	if err != nil {
		return err
	}
	return SelectContext(ctx, ext, ptr, sql, argv...)
}

func NamedSelect(ext sqlx.Ext, ptr interface{}, sql string, args interface{}) error {
	sql, argv, err := ext.BindNamed(sql, args)
	if err != nil {
		return err
	}
	return Select(ext, ptr, sql, argv...)
}

//==============================================================================
//                                Exec
//==============================================================================
func ExecContext(ctx context.Context, ext sqlx.ExtContext, sql string, args ...interface{}) (sql.Result, error) {
	return ext.ExecContext(ctx, sql, args...)
}

func Exec(ext sqlx.Ext, sql string, args ...interface{}) (sql.Result, error) {
	return ext.Exec(sql, args...)
}

func RebindExecContext(ctx context.Context, ext sqlx.ExtContext, sql string, args ...interface{}) (sql.Result, error) {
	return ExecContext(ctx, ext, ext.Rebind(sql), args...)
}

func RebindExec(ext sqlx.Ext, sql string, args ...interface{}) (sql.Result, error) {
	return Exec(ext, ext.Rebind(sql), args...)
}

func NamedExecContext(ctx context.Context, ext sqlx.ExtContext, sql string, args interface{}) (sql.Result, error) {
	return sqlx.NamedExecContext(ctx, ext, sql, args)
}

func NamedExec(ext sqlx.Ext, sql string, args interface{}) (sql.Result, error) {
	return sqlx.NamedExec(ext, sql, args)
}

//==============================================================================
//                                Op
//==============================================================================
type Queryer interface {
	Exec(ext sqlx.Ext) error
	ExecContext(ctx context.Context, ext sqlx.ExtContext) error
}

type nopQueryer struct{}

func NewNopQueryer() Queryer                                                    { return nopQueryer{} }
func (n nopQueryer) Exec(ext sqlx.Ext) error                                    { return nil }
func (n nopQueryer) ExecContext(ctx context.Context, ext sqlx.ExtContext) error { return nil }

type groupQueryers []Queryer

func GroupQueryers(qs ...Queryer) Queryer { return groupQueryers(qs) }
func (g groupQueryers) Exec(ext sqlx.Ext) error {
	for _, q := range g {
		err := q.Exec(ext)
		if err != nil {
			return err
		}
	}
	return nil
}
func (g groupQueryers) ExecContext(ctx context.Context, ext sqlx.ExtContext) error {
	for _, q := range g {
		err := q.ExecContext(ctx, ext)
		if err != nil {
			return err
		}
	}
	return nil
}

type Updater interface {
	Exec(ext sqlx.Ext) (sql.Result, error)
	ExecContext(ctx context.Context, ext sqlx.ExtContext) (sql.Result, error)
}

type groupUpdaters []Updater

func GroupUpdaters(us ...Updater) Updater { return groupUpdaters(us) }

func (g groupUpdaters) Exec(ext sqlx.Ext) (res sql.Result, err error) {
	for _, u := range g {
		res, err = u.Exec(ext)
		if err != nil {
			return res, err
		}
	}
	if res == nil {
		res = fakeResult{}
	}
	return res, nil
}
func (g groupUpdaters) ExecContext(ctx context.Context, ext sqlx.ExtContext) (res sql.Result, err error) {
	for _, u := range g {
		res, err = u.ExecContext(ctx, ext)
		if err != nil {
			return res, err
		}
	}
	if res == nil {
		res = fakeResult{}
	}
	return res, nil
}

type fakeResult struct {
	Affected int64
	LastId   int64
}

func (f fakeResult) LastInsertId() (int64, error) {
	return f.LastId, nil
}

func (f fakeResult) RowsAffected() (int64, error) {
	return f.Affected, nil
}

type nopUpdater struct {
}

func NewNopUpdater() Updater                               { return nopUpdater{} }
func (n nopUpdater) Exec(ext sqlx.Ext) (sql.Result, error) { return fakeResult{}, nil }
func (n nopUpdater) ExecContext(ctx context.Context, ext sqlx.ExtContext) (sql.Result, error) {
	return fakeResult{}, nil
}

type queryerWrapper struct {
	q         Queryer
	doneHooks []func(err error) error
}

func WrapQueryer(q Queryer) queryerWrapper {
	return queryerWrapper{
		q: q,
	}
}

func (w queryerWrapper) Exec(ext sqlx.Ext) error {
	return w.done(w.q.Exec(ext))
}

func (w queryerWrapper) ExecContext(ctx context.Context, ext sqlx.ExtContext) error {
	return w.done(w.q.ExecContext(ctx, ext))
}

func (w queryerWrapper) OnDone(onDone func(err error) error) queryerWrapper {
	w.doneHooks = append(w.doneHooks, onDone)
	return w
}

func (w queryerWrapper) OnSuccess(onSuccess func() error) queryerWrapper {
	return w.OnDone(func(err error) error {
		if err != nil {
			return err
		}
		return onSuccess()
	})
}

func (w queryerWrapper) done(err error) error {
	for _, d := range w.doneHooks {
		err = d(err)
	}
	return err
}

type updaterWrapper struct {
	u         Updater
	doneHooks []func(res sql.Result, err error) (sql.Result, error)
}

func (w updaterWrapper) Exec(ext sqlx.Ext) (sql.Result, error) {
	return w.done(w.u.Exec(ext))
}

func (w updaterWrapper) ExecContext(ctx context.Context, ext sqlx.ExtContext) (sql.Result, error) {
	return w.done(w.u.ExecContext(ctx, ext))
}

func (w updaterWrapper) OnDone(onDone func(res sql.Result, err error) (sql.Result, error)) updaterWrapper {
	w.doneHooks = append(w.doneHooks, onDone)
	return w
}

func (w updaterWrapper) OnSuccess(onSuccess func(res sql.Result) (sql.Result, error)) updaterWrapper {
	return w.OnDone(func(res sql.Result, err error) (sql.Result, error) {
		if err != nil {
			return res, err
		}
		return onSuccess(res)
	})
}

func (w updaterWrapper) done(res sql.Result, err error) (sql.Result, error) {
	for _, d := range w.doneHooks {
		res, err = d(res, err)
	}
	return res, err
}

func WrapUpdater(u Updater) updaterWrapper {
	return updaterWrapper{
		u: u,
	}
}

func OpGet(sql string, ptr interface{}, args ...interface{}) Queryer {
	return opScan{
		query:        Get,
		queryContext: GetContext,
		sql:          sql,
		ptr:          ptr,
		args:         args,
	}
}

func OpRebindGet(sql string, ptr interface{}, args ...interface{}) Queryer {
	return opScan{
		query:        RebindGet,
		queryContext: RebindGetContext,
		sql:          sql,
		ptr:          ptr,
		args:         args,
	}
}

func OpNamedGet(sql string, ptr, args interface{}) Queryer {
	return opNamedScan{
		query:        NamedGet,
		queryContext: NamedGetContext,
		sql:          sql,
		ptr:          ptr,
		args:         args,
	}
}

func OpSelect(sql string, ptr interface{}, args ...interface{}) Queryer {
	return opScan{
		query:        Select,
		queryContext: SelectContext,
		sql:          sql,
		ptr:          ptr,
		args:         args,
	}
}

func OpRebindSelect(sql string, ptr interface{}, args ...interface{}) Queryer {
	return opScan{
		query:        RebindSelect,
		queryContext: RebindSelectContext,
		sql:          sql,
		ptr:          ptr,
		args:         args,
	}
}

func OpNamedSelect(sql string, ptr, args interface{}) Queryer {
	return opNamedScan{
		query:        NamedSelect,
		queryContext: NamedSelectContext,
		sql:          sql,
		ptr:          ptr,
		args:         args,
	}
}

func OpExec(sql string, args ...interface{}) Updater {
	return opUpdate{
		exec:        Exec,
		execContext: ExecContext,
		sql:         sql,
		args:        args,
	}
}

func OpRebindExec(sql string, args ...interface{}) Updater {
	return opUpdate{
		exec:        RebindExec,
		execContext: RebindExecContext,
		sql:         sql,
		args:        args,
	}
}

func OpNamedExec(sql string, args interface{}) Updater {
	return opNamedUpdate{
		exec:        NamedExec,
		execContext: NamedExecContext,
		sql:         sql,
		args:        args,
	}
}

type opNamedScan struct {
	query        func(ext sqlx.Ext, ptr interface{}, sql string, args interface{}) error
	queryContext func(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args interface{}) error
	sql          string
	ptr          interface{}
	args         interface{}
}

func (o opNamedScan) Exec(ext sqlx.Ext) error {
	return o.query(ext, o.ptr, o.sql, o.args)
}

func (o opNamedScan) ExecContext(ctx context.Context, ext sqlx.ExtContext) error {
	return o.queryContext(ctx, ext, o.ptr, o.sql, o.args)
}

type opScan struct {
	query        func(ext sqlx.Ext, ptr interface{}, sql string, args ...interface{}) error
	queryContext func(ctx context.Context, ext sqlx.ExtContext, ptr interface{}, sql string, args ...interface{}) error
	sql          string
	ptr          interface{}
	args         []interface{}
}

func (o opScan) Exec(ext sqlx.Ext) error {
	return o.query(ext, o.ptr, o.sql, o.args...)
}

func (o opScan) ExecContext(ctx context.Context, ext sqlx.ExtContext) error {
	return o.queryContext(ctx, ext, o.ptr, o.sql, o.args...)
}

type opNamedUpdate struct {
	exec        func(ext sqlx.Ext, query string, args interface{}) (sql.Result, error)
	execContext func(ctx context.Context, ext sqlx.ExtContext, query string, args interface{}) (sql.Result, error)
	sql         string
	args        interface{}
}

func (o opNamedUpdate) Exec(ext sqlx.Ext) (sql.Result, error) {
	return o.exec(ext, o.sql, o.args)
}

func (o opNamedUpdate) ExecContext(ctx context.Context, ext sqlx.ExtContext) (sql.Result, error) {
	return o.execContext(ctx, ext, o.sql, o.args)
}

type opUpdate struct {
	exec        func(ext sqlx.Ext, query string, args ...interface{}) (sql.Result, error)
	execContext func(ctx context.Context, ext sqlx.ExtContext, query string, args ...interface{}) (sql.Result, error)

	sql  string
	args []interface{}
}

func (o opUpdate) Exec(ext sqlx.Ext) (sql.Result, error) {
	return o.exec(ext, o.sql, o.args...)
}

func (o opUpdate) ExecContext(ctx context.Context, ext sqlx.ExtContext) (sql.Result, error) {
	return o.execContext(ctx, ext, o.sql, o.args...)
}
