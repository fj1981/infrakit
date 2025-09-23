package cydb

import (
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/duke-git/lancet/v2/slice"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/jmoiron/sqlx"
)

type DBMgr struct {
	dbclis sync.Map
}

func (s *DBMgr) GetCli(key string) *DBCli {
	if v, ok := s.dbclis.Load(key); ok {
		return v.(*DBCli)
	}
	return nil
}

func (s *DBMgr) SetCli(key string, cli *DBCli) {
	c, loaded := s.dbclis.LoadAndDelete(key)
	if loaded && c != nil {
		cli2, ok := c.(*DBCli)
		if ok {
			_ = cli2.Close()
		}
		cli.key = key
	}
	s.dbclis.Store(key, cli)
}

func (s *DBMgr) GetOrCreateCli(params map[string]interface{}) (*DBCli, error) {

	key := cyutil.MD5(params)
	if v, ok := s.dbclis.Load(key); ok {
		return v.(*DBCli), nil
	}
	cfg, err := cyutil.MapToStruct[DBConnection](params)
	if err != nil {
		return nil, err
	}
	cli, err := TryConnect(cfg)
	if err != nil {
		return nil, err
	}
	cli.key = key
	s.SetCli(key, cli)
	return cli, nil
}

func (s *DBMgr) CloseAll() {
	s.dbclis.Range(func(k, v interface{}) bool {
		cli, ok := v.(*DBCli)
		if !ok {
			return true
		}
		_ = cli.Close()
		return true
	})
}

func (s *DBMgr) InitByConfig(conf *Config) error {
	for _, v := range conf.Connections {
		cli, err := TryConnect(&v)
		if err != nil {
			s.CloseAll()
			return err
		}
		if v.Key == "" {
			v.Key = cyutil.MD5(v)
		}
		s.SetCli(v.Key, cli)
		if v.DBName != "" && v.DBName != v.Key {
			s.SetCli(v.DBName, cli)
		}
	}
	return nil
}

func TryConnect(v *DBConnection) (*DBCli, error) {
	if sqlFunc, ok := GetSqlDialect(v.Type); ok {
		driverName, conn := sqlFunc.GetConnectStr(v)
		sqlxDB, err := sqlx.Open(driverName, conn)
		if err != nil {
			return nil, err
		}
		maxIdleConns := 5
		maxOpenConns := 10
		maxLifetime := 600
		sqlxDB.SetMaxIdleConns(maxIdleConns)
		sqlxDB.SetMaxOpenConns(maxOpenConns)
		sqlxDB.SetConnMaxLifetime(time.Duration(maxLifetime) * time.Second)
		err = sqlxDB.Ping()
		if err != nil {
			return nil, err
		}
		return &DBCli{cli: sqlxDB, key: v.Key, dbtype: v.Type, database: v.DBName, un: v.Un, pw: v.Pw}, err
	}
	return nil, errors.New("db type not found")
}

type MigrateSQLParam struct {
	fs           *embed.FS
	ignoreError  bool
	serviceOwner string
}

type MigrateFileFunc func(pm *MigrateSQLParam) *MigrateSQLParam

func WithMigrateFileFunc(f *embed.FS) MigrateFileFunc {
	return func(pm *MigrateSQLParam) *MigrateSQLParam {
		pm.fs = f
		return pm
	}
}

func WithIgnoreError(ignoreError bool) MigrateFileFunc {
	return func(pm *MigrateSQLParam) *MigrateSQLParam {
		pm.ignoreError = ignoreError
		return pm
	}
}

func WithServiceOwner(serviceOwner string) MigrateFileFunc {
	return func(pm *MigrateSQLParam) *MigrateSQLParam {
		pm.serviceOwner = serviceOwner
		return pm
	}
}
func LeafDirs(efs *embed.FS) ([]string, error) {
	var leafDirs []string
	err := fs.WalkDir(efs, ".", func(filePath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			entries, err := fs.ReadDir(efs, filePath)
			if err != nil {
				return err
			}

			hasSubDir := false
			for _, entry := range entries {
				if entry.IsDir() {
					hasSubDir = true
					break
				}
			}

			if !hasSubDir {
				leafDirs = append(leafDirs, filePath)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return leafDirs, nil
}

type MigrateInfo struct {
	Key    string
	DBType string
}

func substractMigrateInfo(dir string) MigrateInfo {
	parts := strings.Split(dir, "/")
	ret := MigrateInfo{}
	if len(parts) < 2 {
		return ret
	}
	ret.Key = parts[len(parts)-1]
	supportDBType := GetSupportSqlDialect()
	ret.DBType = strings.ToLower(parts[len(parts)-2])
	if !slice.Contain(supportDBType, ret.DBType) {
		ret.DBType = ""
	}
	return ret
}

func NewSqlMgr(conf *Config, migrateFileFunc ...MigrateFileFunc) (*DBMgr, error) {
	s := &DBMgr{}
	err := s.InitByConfig(conf)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.CloseAll()
		}
	}()
	pm := &MigrateSQLParam{}
	for _, f := range migrateFileFunc {
		pm = f(pm)
		if pm == nil {
			continue
		}
	}
	if pm.fs != nil {
		dirs, err := LeafDirs(pm.fs)
		if err != nil {
			return nil, err
		}
		for _, dir := range dirs {
			mi := substractMigrateInfo(dir)
			cli := s.GetCli(mi.Key)
			if cli == nil {
				slog.Warn("migrate from folder failed", "dir", dir, "err", "cli not found")
				continue
			}
			if mi.DBType == "" || mi.DBType == cli.dbtype {
				err = cli.migrateFromFolder(pm, dir)
				if err != nil {
					if pm.ignoreError {
						slog.Error("migrate from folder failed", "dir", dir, "err", err)
						continue
					}
					return nil, err
				}
			}
		}
	}
	return s, nil
}
