package cydb

import (
	"fmt"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

type IRepo interface {
	GetDBCli() *DBCli
	SetDBCli(*DBCli)
}

func WithTransaction[T IRepo](repo T, fn func(repo T) error) error {
	tx := repo.GetDBCli()
	if tx == nil {
		return fmt.Errorf("failed to begin transaction")
	}
	return tx.WithTransaction(func(tx *DBCli) error {
		tempRepo, b := cyutil.NewObj[T]()
		if !b {
			return fmt.Errorf("failed to create repo")
		}
		tempRepo.SetDBCli(tx)
		return fn(tempRepo)
	})
}
